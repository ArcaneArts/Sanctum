use std::io::{BufRead, Read, Write};

use encoding_rs::{Decoder, DecoderResult, Encoding, WINDOWS_1252};
use sanctum_core::{Error, Result};

pub const RTF_EXTRACTOR: &str = "rtf-text-v1";
const MAX_DEPTH: usize = 256;
const MAX_CONTROL: usize = 32;

#[derive(Clone, Copy)]
struct Group {
    hidden: bool,
    unicode_fallback: usize,
    alternate_child: Option<u8>,
    unicode_alternate: bool,
}

pub fn rtf_to_utf8(input: impl BufRead, output: impl Write) -> Result<&'static str> {
    let mut parser = Parser {
        input,
        output,
        groups: Vec::new(),
        state: Group {
            hidden: false,
            unicode_fallback: 1,
            alternate_child: None,
            unicode_alternate: false,
        },
        decoder: WINDOWS_1252.new_decoder_without_bom_handling(),
        encoding: WINDOWS_1252,
        fallback: 0,
        surrogate: None,
    };
    parser.document()?;
    Ok(parser.encoding.name())
}

struct Parser<R, W> {
    input: R,
    output: W,
    groups: Vec<Group>,
    state: Group,
    decoder: Decoder,
    encoding: &'static Encoding,
    fallback: usize,
    surrogate: Option<u16>,
}

impl<R: BufRead, W: Write> Parser<R, W> {
    fn document(&mut self) -> Result<()> {
        let mut header = [0; 6];
        self.input.read_exact(&mut header).map_err(|error| {
            if error.kind() == std::io::ErrorKind::UnexpectedEof {
                invalid("missing RTF header")
            } else {
                error.into()
            }
        })?;
        if &header != br"{\rtf1"
            || self
                .peek()?
                .is_some_and(|byte| byte.is_ascii_digit() || byte.is_ascii_alphabetic())
        {
            return Err(invalid("expected an RTF version 1 document"));
        }
        if self.peek()? == Some(b' ') {
            self.byte()?;
        }
        self.groups.push(self.state);
        while let Some(byte) = self.byte()? {
            match byte {
                b'{' => {
                    self.flush_ansi()?;
                    self.fallback = 0;
                    if self.groups.len() == MAX_DEPTH {
                        return Err(invalid("RTF nesting limit exceeded"));
                    }
                    let alternate = self.state.alternate_child;
                    self.state.alternate_child = alternate.map(|child| child.saturating_add(1));
                    self.groups.push(self.state);
                    self.state.alternate_child = None;
                    self.state.unicode_alternate = alternate == Some(1) && !self.state.hidden;
                    if alternate == Some(0) {
                        self.state.hidden = true;
                    }
                }
                b'}' => {
                    self.flush_ansi()?;
                    self.fallback = 0;
                    self.state = self.groups.pop().expect("document group remains open");
                    if self.groups.is_empty() {
                        if self.surrogate.is_some() {
                            return Err(invalid("incomplete RTF Unicode surrogate pair"));
                        }
                        while let Some(byte) = self.byte()? {
                            if !byte.is_ascii_whitespace() {
                                return Err(invalid("unexpected data after the RTF document"));
                            }
                        }
                        self.output.flush()?;
                        return Ok(());
                    }
                }
                b'\\' => self.control()?,
                b'\r' | b'\n' => {}
                byte => self.text_byte(byte)?,
            }
        }
        Err(invalid("unclosed RTF group"))
    }

    fn control(&mut self) -> Result<()> {
        let first = self.required_byte()?;
        if !first.is_ascii_alphabetic() {
            return match first {
                b'\'' => {
                    let high = hex(self.required_byte()?)?;
                    let low = hex(self.required_byte()?)?;
                    self.text_byte(high * 16 + low)
                }
                b'\\' | b'{' | b'}' => self.text_byte(first),
                b'~' => self.symbol('\u{a0}'),
                b'_' => self.symbol('\u{2011}'),
                b'-' => self.symbol('\u{ad}'),
                b'\r' | b'\n' => self.symbol('\n'),
                b'*' => {
                    self.flush_ansi()?;
                    if !self.state.unicode_alternate {
                        self.state.hidden = true;
                    }
                    Ok(())
                }
                _ => {
                    self.skip_fallback();
                    Ok(())
                }
            };
        }
        let mut word = [0; MAX_CONTROL];
        word[0] = first;
        let mut len = 1;
        while self.peek()?.is_some_and(|byte| byte.is_ascii_alphabetic()) {
            if len == word.len() {
                return Err(invalid("RTF control word is too long"));
            }
            word[len] = self.required_byte()?;
            len += 1;
        }
        let negative = self.peek()? == Some(b'-');
        if negative {
            self.byte()?;
        }
        let mut number = None;
        while let Some(byte) = self.peek()?.filter(u8::is_ascii_digit) {
            self.byte()?;
            let value = number
                .unwrap_or(0i32)
                .checked_mul(10)
                .and_then(|value| value.checked_add(i32::from(byte - b'0')))
                .ok_or_else(|| invalid("RTF control number is out of range"))?;
            number = Some(value);
        }
        if negative {
            number = Some(-number.ok_or_else(|| invalid("missing RTF control number"))?);
        }
        if self.peek()? == Some(b' ') {
            self.byte()?;
        }
        self.word(&word[..len], number)
    }

    fn word(&mut self, word: &[u8], number: Option<i32>) -> Result<()> {
        if self.state.unicode_alternate {
            self.state.unicode_alternate = false;
            if word != b"ud" {
                return Err(invalid("invalid RTF Unicode alternate destination"));
            }
        }
        if word == b"bin" {
            let bytes = number
                .filter(|n| *n >= 0)
                .ok_or_else(|| invalid("invalid RTF binary length"))?;
            let copied = std::io::copy(
                &mut Read::take(&mut self.input, bytes as u64),
                &mut std::io::sink(),
            )?;
            if copied != bytes as u64 {
                return Err(invalid("truncated RTF binary data"));
            }
            self.skip_fallback();
            return Ok(());
        }
        if self.state.hidden || self.skip_fallback() {
            return Ok(());
        }
        match word {
            b"fonttbl" | b"colortbl" | b"stylesheet" | b"info" | b"pict" | b"object"
            | b"filetbl" | b"header" | b"headerl" | b"headerr" | b"headerf" | b"footer"
            | b"footerl" | b"footerr" | b"footerf" | b"fldinst" | b"listtable"
            | b"listoverridetable" | b"themedata" | b"datastore" | b"annotation" => {
                self.flush_ansi()?;
                self.state.hidden = true;
            }
            b"ansi" => self.set_encoding(WINDOWS_1252)?,
            b"mac" => self.set_encoding(encoding(b"macintosh")?)?,
            b"pc" | b"pca" => return Err(invalid("unsupported RTF DOS code page")),
            b"ansicpg" => self.set_encoding(code_page(number)?)?,
            b"upr" => self.state.alternate_child = Some(0),
            b"uc" => {
                self.state.unicode_fallback = number
                    .filter(|n| (0..=255).contains(n))
                    .ok_or_else(|| invalid("invalid RTF Unicode fallback length"))?
                    as usize;
            }
            b"u" => {
                self.flush_ansi()?;
                let unit = number
                    .filter(|n| (-32768..=65535).contains(n))
                    .ok_or_else(|| invalid("invalid RTF Unicode code unit"))?
                    as u16;
                self.unicode_unit(unit)?;
                self.fallback = self.state.unicode_fallback;
            }
            b"par" | b"line" | b"row" | b"page" => self.output_char('\n')?,
            b"tab" | b"cell" => self.output_char('\t')?,
            b"emdash" => self.output_char('\u{2014}')?,
            b"endash" => self.output_char('\u{2013}')?,
            b"bullet" => self.output_char('\u{2022}')?,
            b"lquote" => self.output_char('\u{2018}')?,
            b"rquote" => self.output_char('\u{2019}')?,
            b"ldblquote" => self.output_char('\u{201c}')?,
            b"rdblquote" => self.output_char('\u{201d}')?,
            _ => {}
        }
        Ok(())
    }

    fn unicode_unit(&mut self, unit: u16) -> Result<()> {
        match (self.surrogate.take(), unit) {
            (None, 0xd800..=0xdbff) => self.surrogate = Some(unit),
            (Some(high), 0xdc00..=0xdfff) => {
                let scalar =
                    0x10000 + ((u32::from(high) - 0xd800) << 10) + u32::from(unit) - 0xdc00;
                self.output_char(char::from_u32(scalar).expect("paired surrogates are valid"))?;
            }
            (None, 0xdc00..=0xdfff) | (Some(_), _) => {
                return Err(invalid("unpaired RTF Unicode surrogate"));
            }
            (None, _) => {
                self.output_char(char::from_u32(u32::from(unit)).expect("valid BMP scalar"))?
            }
        }
        Ok(())
    }

    fn output_char(&mut self, value: char) -> Result<()> {
        self.flush_ansi()?;
        if value == '\0' || self.surrogate.is_some() {
            return Err(invalid("invalid RTF Unicode text"));
        }
        self.output
            .write_all(value.encode_utf8(&mut [0; 4]).as_bytes())?;
        Ok(())
    }

    fn symbol(&mut self, value: char) -> Result<()> {
        if !self.state.hidden && !self.skip_fallback() {
            self.output_char(value)?;
        }
        Ok(())
    }

    fn text_byte(&mut self, byte: u8) -> Result<()> {
        if self.state.hidden || self.skip_fallback() {
            return Ok(());
        }
        if byte == 0 || self.surrogate.is_some() {
            return Err(invalid("invalid RTF text byte"));
        }
        self.decode(&[byte], false)
    }

    fn decode(&mut self, bytes: &[u8], last: bool) -> Result<()> {
        let mut output = [0; 16];
        let (result, read, written) =
            self.decoder
                .decode_to_utf8_without_replacement(bytes, &mut output, last);
        if result != DecoderResult::InputEmpty || read != bytes.len() {
            return Err(invalid("malformed text for the declared RTF code page"));
        }
        self.output.write_all(&output[..written])?;
        Ok(())
    }

    fn flush_ansi(&mut self) -> Result<()> {
        self.decode(&[], true)?;
        self.decoder = self.encoding.new_decoder_without_bom_handling();
        Ok(())
    }

    fn set_encoding(&mut self, encoding: &'static Encoding) -> Result<()> {
        self.flush_ansi()?;
        self.encoding = encoding;
        self.decoder = encoding.new_decoder_without_bom_handling();
        Ok(())
    }

    fn skip_fallback(&mut self) -> bool {
        let skip = self.fallback != 0;
        self.fallback = self.fallback.saturating_sub(1);
        skip
    }

    fn peek(&mut self) -> Result<Option<u8>> {
        Ok(self.input.fill_buf()?.first().copied())
    }

    fn byte(&mut self) -> Result<Option<u8>> {
        let byte = self.peek()?;
        if byte.is_some() {
            self.input.consume(1);
        }
        Ok(byte)
    }

    fn required_byte(&mut self) -> Result<u8> {
        self.byte()?.ok_or_else(|| invalid("truncated RTF escape"))
    }
}

fn code_page(number: Option<i32>) -> Result<&'static Encoding> {
    match number {
        Some(n @ 1250..=1258) => encoding(format!("windows-{n}").as_bytes()),
        Some(65001) => encoding(b"utf-8"),
        Some(932) => encoding(b"shift_jis"),
        Some(936) => encoding(b"gbk"),
        Some(949) => encoding(b"euc-kr"),
        Some(950) => encoding(b"big5"),
        Some(874) => encoding(b"windows-874"),
        Some(10000) => encoding(b"macintosh"),
        _ => Err(invalid("unsupported RTF code page")),
    }
}

fn encoding(label: &[u8]) -> Result<&'static Encoding> {
    Encoding::for_label(label).ok_or_else(|| invalid("unsupported RTF code page"))
}

fn hex(byte: u8) -> Result<u8> {
    (byte as char)
        .to_digit(16)
        .map(|value| value as u8)
        .ok_or_else(|| invalid("invalid RTF hex escape"))
}

fn invalid(message: &str) -> Error {
    Error::Invalid(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn extract(bytes: &[u8]) -> Result<String> {
        let mut out = Vec::new();
        rtf_to_utf8(std::io::BufReader::with_capacity(2, bytes), &mut out)?;
        Ok(String::from_utf8(out).unwrap())
    }

    #[test]
    fn handles_destinations_code_pages_and_unicode_across_input_buffers() {
        let doc = br"{\rtf1\ansi\ansicpg1252{\fonttbl{\f0 HiddenFont;}}{\*\generator HiddenGenerator;}Caf\'e9\par \uc1\u28450? \u-10179?\u-8704? {\upr{ANSI}{\*\ud \u937?}} {\field{\*\fldinst HYPERLINK hidden}{\fldrslt visible}}{\pict\bin4 }{\x}}";
        assert_eq!(extract(doc).unwrap(), "Café\n漢 😀 Ω visible");
        assert_eq!(extract(br"{\rtf1\ansi\ansicpg932\'82\'a0}").unwrap(), "あ");
    }

    #[test]
    fn rejects_invalid_or_unsupported_rtf_instead_of_emitting_partial_text() {
        for input in [
            &br"{\rtf1 unterminated"[..],
            &br"{\rtf1\ansicpg9876 text}"[..],
            &br"{\rtf1\ansi\ansicpg932\'82}"[..],
            &br"{\rtf1{\pict\bin99 bytes}}"[..],
            &br"{\rtf1\uc1\u-10179?}"[..],
        ] {
            assert!(matches!(extract(input), Err(Error::Invalid(_))));
        }
    }
}
