package it.unimi.di.law.bubing.util.cld2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;

public class Cld2Tool
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Cld2Tool.class);
    private static final ThreadLocal<CharsetEncoder> utf8Encoders = ThreadLocal.withInitial( () -> Charset.forName("utf-8").newEncoder() );

    /**
     * using jna, calls into the c++ code to retrieve the language code based on the integer language enum value.
     * the name of the C++ method is mangled
     * @param lang3
     * @return String[] containing the codes
     */
    public static String[] getLanguageCodes(Cld2Library cld2Library, int[] lang3){
        String[] codes = new String[3];
        for(int i=0; i<3; i++){
            codes[i] = cld2Library._ZN4CLD212LanguageCodeENS_8LanguageE(lang3[i]);
        }
        return codes;
    }


    /**
     * using jna, calls into the c++ code to retrieve the language name based on the integer language enum value.
     * @param lang3
     * @return String[] containing the language names
     */
    public static String[] getLanguageNames(Cld2Library cld2Library, int[] lang3){
        String[] names = new String[3];
        for(int i=0; i<3; i++){
            names[i] = cld2Library._ZN4CLD212LanguageNameENS_8LanguageE(lang3[i]);
        }
        return names;
    }

    private static final class Cld2BugWrapper implements CharSequence
    {
        private final CharSequence seq;
        private final int length;

        Cld2BugWrapper( final CharSequence seq, final int length ) {
            this.seq = seq;
            this.length = Math.min( seq.length(), length );
        }

        @Override
        public final int length() {
            return length+1; // Workaround CLD2 bug (SIGSEGV) part 1.
        }

        @Override
        public final char charAt( int index ) {
            return index == length ? ' ' : seq.charAt( index ); // Workaround CLD2 bug (SIGSEGV) part 2.
        }

        @Override
        public final CharSequence subSequence( int start, int end ) {
            return CharBuffer.wrap( this, start, end );
        }
    }

    /**
     * Public interface for the CLD2 library.
     */
        public static int getLanguageFromName(String name) {
            return Cld2Library.INSTANCE._ZN4CLD219GetLanguageFromNameEPKc(name);
        }

        public static String getLanguageName(int language) {
            return Cld2Library.INSTANCE._ZN4CLD212LanguageNameENS_8LanguageE(language);
        }

        public static String getLanguageCode(int language) {
            return Cld2Library.INSTANCE._ZN4CLD212LanguageCodeENS_8LanguageE(language);
        }

        public static String version() {
            return Cld2Library.INSTANCE._ZN4CLD221DetectLanguageVersionEv();
        }

        public static Cld2Result detect( final CharSequence text, final int length, final String tld, final String hintLang ) {
            try {
                final CharsetEncoder encoder = utf8Encoders.get();
                final CharBuffer charBuffer = CharBuffer.wrap( new Cld2BugWrapper(text,length) );
                final ByteBuffer byteBuffer = encoder.encode( charBuffer );
                return detectImpl( byteBuffer.array(), byteBuffer.remaining(), hintLang, tld );
            }
            catch ( java.nio.charset.CharacterCodingException e ) {
                LOGGER.warn( "Failed to encode to UTF-8, the Charset was probably wrong");
                return new Cld2Result( "Unknown", "Unknown", 0.0 );
            }
        }

        private static Cld2Result detectImpl( final byte[] bytes, final int length, final String hintLang, final String tld ) {
            boolean isPlainText = true;
            Cld2Hints cld2Hints = new Cld2Hints(
              hintLang,
              tld,
              Cld2Encoding.UNKNOWN_ENCODING,
              Cld2Language.UNKNOWN_LANGUAGE
            );

            int flags = 0;
            int[] language3 = new int[3];
            int[] percent3 = new int[3];
            double[] normalizedScore3 = new double[3];
            int[] textBytes = new int[1];
            boolean[] isReliable = new boolean[1];

            int language = Cld2Library.INSTANCE._ZN4CLD224ExtDetectLanguageSummaryEPKcibPKNS_8CLDHintsEiPNS_8LanguageEPiPdPSt6vectorINS_11ResultChunkESaISA_EES7_Pb(
              bytes,
              length-1, // Workaround CLD2 bug (SIGSEGV) part 3.
              isPlainText,
              cld2Hints,
              flags,
              language3,
              percent3,
              normalizedScore3,
              null, // Supposed to be a vector of ResultChunks, but it is not direct to pass vectors.
              textBytes,
              isReliable);
            return new Cld2Result(
              getLanguageName(language),
              getLanguageCode(language),
              percent3[0] / 100.0
            );
        }

        /*
        public static Cld2Result detect(String text, String tld, String hintLang) {
            boolean isPlainText = true;
            Cld2Hints cld2Hints = new Cld2Hints(
                    hintLang,
                    tld,
                    Cld2Encoding.UNKNOWN_ENCODING,
                    Cld2Language.UNKNOWN_LANGUAGE);
            int flags = 0;
            int[] language3 = new int[3];
            int[] percent3 = new int[3];
            double[] normalizedScore3 = new double[3];
            int[] textBytes = new int[1];
            boolean[] isReliable = new boolean[1];
            byte[] utf8EncodedText;
            try {
                utf8EncodedText = text.getBytes("UTF-8");
            } catch (java.io.UnsupportedEncodingException exc) {
                return new Cld2Result("UNKNOWN", "UNKNOWN", 0.0);
            }
            int language = Cld2Library.INSTANCE._ZN4CLD224ExtDetectLanguageSummaryEPKcibPKNS_8CLDHintsEiPNS_8LanguageEPiPdPSt6vectorINS_11ResultChunkESaISA_EES7_Pb(
                    utf8EncodedText,
                    utf8EncodedText.length-1, // Workaround CLD2 bug (SIGSEGV) part 2.
                    isPlainText,
                    cld2Hints,
                    flags,
                    language3,
                    percent3,
                    normalizedScore3,
                    null, // Supposed to be a vector of ResultChunks, but it is not direct to pass vectors.
                    textBytes,
                    isReliable);

            return new Cld2Result(
                    getLanguageName(language), getLanguageCode(language), percent3[0] / 100.0);
        }
        */
}
