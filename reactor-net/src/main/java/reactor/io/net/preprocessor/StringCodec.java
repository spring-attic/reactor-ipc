package reactor.io.net.preprocessor;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import reactor.fn.Function;
import reactor.fn.Supplier;
import reactor.io.codec.Codec;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;

public class StringCodec extends Codec<ByteBuf, String, String> {

    private final Charset charset;

    public StringCodec() {
        this(null, Charset.forName("UTF-8"));
    }

    public StringCodec(Charset charset) {
        this(null, charset);
    }

    public StringCodec(Byte delimiter) {
        this(delimiter, Charset.forName("UTF-8"));
    }

    public StringCodec(Byte delimiter, final Charset charset) {
        super(delimiter, new Supplier<CharsetDecoder>(){
            @Override
            public CharsetDecoder get() {
                return charset.newDecoder();
            }
        });
        this.charset = charset;
    }

    @Override
    public Function<String, ByteBuf> encoder() {
        return new StringEncoder();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected String decodeNext(ByteBuf buffer, Object context) {
        ByteBuf b = buffer;
        if(delimiter != null){
            int end = buffer.indexOf(buffer.readerIndex(), buffer.capacity(), delimiter);
            if(end != - 1) {
                b = buffer.duplicate().slice(buffer.readerIndex(), end - 1);
                buffer.readerIndex((Math.min(buffer.capacity(), end + 1) - buffer.readerIndex()));
            }
        }
        return decode(b, (CharsetDecoder)context);
    }



    @Override
    public ByteBuf apply(String s) {
        return encode(s, charset.newEncoder());
    }

    protected String decode(ByteBuf buffer, CharsetDecoder charsetDecoder) {
        try {
            return charsetDecoder.decode(buffer.nioBuffer()).toString();
        } catch (CharacterCodingException e) {
            throw new IllegalStateException(e);
        }
    }

    protected ByteBuf encode(String s, CharsetEncoder charsetEncoder) {
        try {
            ByteBuffer bb = charsetEncoder.encode(CharBuffer.wrap(s));
            if (delimiter != null) {
                bb.put(delimiter);
                bb.flip();
                return Unpooled.wrappedBuffer(bb);
            } else {
                return Unpooled.wrappedBuffer(bb);
            }
        } catch (CharacterCodingException e) {
            throw new IllegalStateException(e);
        }
    }

    public final class StringEncoder implements Function<String, ByteBuf> {
        private final CharsetEncoder encoder = charset.newEncoder();

        @Override
        public ByteBuf apply(String s) {
            return encode(s, encoder);
        }
    }

}

