use capnp::private::layout::{
    PointerBuilder, PointerReader, StructBuilder, StructReader, StructSize,
};
use capnp::traits::{
    FromPointerBuilder, FromPointerReader, HasStructSize, IntoInternalStructReader,
};
use capnp::{Result, Word, text, text_list};

pub mod cli_request {
    use super::*;

    const STRUCT_SIZE: StructSize = StructSize {
        data: 0,
        pointers: 1,
    };

    #[derive(Clone, Copy)]
    pub struct Reader<'a> {
        reader: StructReader<'a>,
    }

    impl<'a> From<StructReader<'a>> for Reader<'a> {
        fn from(reader: StructReader<'a>) -> Self {
            Self { reader }
        }
    }

    impl<'a> IntoInternalStructReader<'a> for Reader<'a> {
        fn into_internal_struct_reader(self) -> StructReader<'a> {
            self.reader
        }
    }

    impl<'a> FromPointerReader<'a> for Reader<'a> {
        fn get_from_pointer(
            reader: &PointerReader<'a>,
            default: Option<&'a [Word]>,
        ) -> Result<Self> {
            Ok(Self {
                reader: reader.get_struct(default)?,
            })
        }
    }

    impl<'a> Reader<'a> {
        pub fn get_args(self) -> Result<text_list::Reader<'a>> {
            text_list::Reader::get_from_pointer(&self.reader.get_pointer_field(0), None)
        }
    }

    pub struct Builder<'a> {
        builder: StructBuilder<'a>,
    }

    impl<'a> From<StructBuilder<'a>> for Builder<'a> {
        fn from(builder: StructBuilder<'a>) -> Self {
            Self { builder }
        }
    }

    impl<'a> HasStructSize for Builder<'a> {
        const STRUCT_SIZE: StructSize = STRUCT_SIZE;
    }

    impl<'a> FromPointerBuilder<'a> for Builder<'a> {
        fn init_pointer(builder: PointerBuilder<'a>, _len: u32) -> Self {
            Self {
                builder: builder.init_struct(STRUCT_SIZE),
            }
        }

        fn get_from_pointer(
            builder: PointerBuilder<'a>,
            default: Option<&'a [Word]>,
        ) -> Result<Self> {
            Ok(Self {
                builder: builder.get_struct(STRUCT_SIZE, default)?,
            })
        }
    }

    impl<'a> Builder<'a> {
        pub fn init_args(&mut self, len: u32) -> text_list::Builder<'_> {
            text_list::Builder::init_pointer(self.builder.reborrow().get_pointer_field(0), len)
        }
    }
}

pub mod cli_response {
    use super::*;

    const STRUCT_SIZE: StructSize = StructSize {
        data: 1,
        pointers: 2,
    };

    #[derive(Clone, Copy)]
    pub struct Reader<'a> {
        reader: StructReader<'a>,
    }

    impl<'a> From<StructReader<'a>> for Reader<'a> {
        fn from(reader: StructReader<'a>) -> Self {
            Self { reader }
        }
    }

    impl<'a> IntoInternalStructReader<'a> for Reader<'a> {
        fn into_internal_struct_reader(self) -> StructReader<'a> {
            self.reader
        }
    }

    impl<'a> FromPointerReader<'a> for Reader<'a> {
        fn get_from_pointer(
            reader: &PointerReader<'a>,
            default: Option<&'a [Word]>,
        ) -> Result<Self> {
            Ok(Self {
                reader: reader.get_struct(default)?,
            })
        }
    }

    impl<'a> Reader<'a> {
        pub fn get_exit_code(self) -> i32 {
            self.reader.get_data_field::<i32>(0)
        }

        pub fn get_stdout(self) -> Result<text::Reader<'a>> {
            text::Reader::get_from_pointer(&self.reader.get_pointer_field(0), None)
        }

        pub fn get_stderr(self) -> Result<text::Reader<'a>> {
            text::Reader::get_from_pointer(&self.reader.get_pointer_field(1), None)
        }
    }

    pub struct Builder<'a> {
        builder: StructBuilder<'a>,
    }

    impl<'a> From<StructBuilder<'a>> for Builder<'a> {
        fn from(builder: StructBuilder<'a>) -> Self {
            Self { builder }
        }
    }

    impl<'a> HasStructSize for Builder<'a> {
        const STRUCT_SIZE: StructSize = STRUCT_SIZE;
    }

    impl<'a> FromPointerBuilder<'a> for Builder<'a> {
        fn init_pointer(builder: PointerBuilder<'a>, _len: u32) -> Self {
            Self {
                builder: builder.init_struct(STRUCT_SIZE),
            }
        }

        fn get_from_pointer(
            builder: PointerBuilder<'a>,
            default: Option<&'a [Word]>,
        ) -> Result<Self> {
            Ok(Self {
                builder: builder.get_struct(STRUCT_SIZE, default)?,
            })
        }
    }

    impl<'a> Builder<'a> {
        pub fn set_exit_code(&mut self, value: i32) {
            self.builder.set_data_field::<i32>(0, value);
        }

        pub fn set_stdout(&mut self, value: &str) {
            self.builder
                .reborrow()
                .get_pointer_field(0)
                .set_text(text::Reader::from(value));
        }

        pub fn set_stderr(&mut self, value: &str) {
            self.builder
                .reborrow()
                .get_pointer_field(1)
                .set_text(text::Reader::from(value));
        }
    }
}
