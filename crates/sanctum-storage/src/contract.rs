use bytes::Bytes;
use sanctum_core::{ByteRange, Error};
use tokio::io::{AsyncReadExt, repeat};

use crate::{Condition, Key, Storage, usage};

pub async fn verify(storage: &dyn Storage, base: &str) {
    let prefix = Key::parse(format!("{base}/notes")).unwrap();
    let sibling = Key::parse(format!("{base}/notes-other/ignore")).unwrap();
    let control = Key::parse(format!("{prefix}/HEAD.json")).unwrap();
    let source = Key::parse(format!("{prefix}/sources/text.utf8")).unwrap();
    let bulk = Key::parse(format!("{prefix}/snapshots/meta.db")).unwrap();
    let invalid = Key::parse(format!("{prefix}/staging/short")).unwrap();

    let version = storage
        .put_control(&control, Bytes::from_static(b"original"), Condition::Create)
        .await
        .unwrap();
    assert!(matches!(
        storage
            .put_control(&control, Bytes::from_static(b"conflict"), Condition::Create)
            .await,
        Err(Error::Conflict(_))
    ));
    let (left, right) = tokio::join!(
        storage.put_control(
            &control,
            Bytes::from_static(b"left"),
            Condition::Match(version.clone())
        ),
        storage.put_control(
            &control,
            Bytes::from_static(b"right"),
            Condition::Match(version)
        ),
    );
    assert_ne!(left.is_ok(), right.is_ok(), "{left:?} / {right:?}");
    assert!(matches!(
        left.as_ref().err().or(right.as_ref().err()),
        Some(Error::Conflict(_))
    ));

    let text = "abc 🧭 café\n".as_bytes();
    storage
        .put_stream(&source, Box::pin(text), text.len() as u64)
        .await
        .unwrap();
    storage
        .put_stream(&sibling, Box::pin(&b"ignored"[..]), 7)
        .await
        .unwrap();
    assert_eq!(
        storage.head(&source).await.unwrap().length,
        text.len() as u64
    );
    assert_eq!(
        storage
            .read_range(&source, ByteRange { start: 4, end: 8 })
            .await
            .unwrap(),
        Bytes::from_static("🧭".as_bytes())
    );
    assert!(
        storage
            .read_range(&source, ByteRange { start: 4, end: 100 })
            .await
            .is_err()
    );
    assert!(
        storage
            .put_stream(&invalid, Box::pin(&b"x"[..]), 2)
            .await
            .is_err()
    );
    assert!(matches!(
        storage.head(&invalid).await,
        Err(Error::NotFound(_))
    ));

    // Forces multiple upload parts without allocating the whole input.
    let length = 17 * 1024 * 1024 + 7;
    storage
        .put_stream(&bulk, Box::pin(repeat(b'z').take(length)), length)
        .await
        .unwrap();
    let mut download = storage.get(&bulk).await.unwrap();
    assert_eq!(download.object.length, length);
    let mut buffer = [0; 64 * 1024];
    let mut read = 0;
    loop {
        let count = download.reader.read(&mut buffer).await.unwrap();
        if count == 0 {
            break;
        }
        assert!(buffer[..count].iter().all(|byte| *byte == b'z'));
        read += count as u64;
    }
    assert_eq!(read, length);

    let mut cursor = None;
    let mut listed = Vec::new();
    loop {
        let page = storage
            .list(Some(&prefix), cursor.as_deref(), 1)
            .await
            .unwrap();
        assert!(page.objects.len() <= 1);
        listed.extend(page.objects.into_iter().map(|object| object.key));
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    listed.sort();
    assert_eq!(listed, [control.clone(), bulk.clone(), source.clone()]);
    let measured = usage(storage, Some(&prefix)).await.unwrap();
    assert_eq!(measured.files, 3);
    let head = storage.head(&control).await.unwrap();
    assert_eq!(measured.bytes, length + text.len() as u64 + head.length);
    assert_eq!(
        usage(storage, Some(&Key::parse(base).unwrap()))
            .await
            .unwrap()
            .bytes,
        measured.bytes + 7
    );

    for key in [control, source, bulk, sibling] {
        storage.delete(&key).await.unwrap();
        storage.delete(&key).await.unwrap();
        assert!(matches!(storage.head(&key).await, Err(Error::NotFound(_))));
    }
}
