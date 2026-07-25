package demo;

import java.io.*;
import java.nio.file.Files;
import java.util.*;

/** Corrupts a single record's value in a shard's .bin (simulates S3 bit-rot / a bad backup). */
public final class CorruptRecord {
    public static void main(String[] args) throws Exception {
        String dataDir = args[0];
        int shard = Integer.parseInt(args[1]);
        long targetSeq = Long.parseLong(args[2]);
        File dir = new File(dataDir, "shard=" + shard);
        for (File f : Objects.requireNonNull(dir.listFiles((d, n) -> n.endsWith(".bin")))) {
            byte[] bytes = Files.readAllBytes(f.toPath());
            boolean changed = false;
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream o = new DataOutputStream(bos);
            try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
                o.writeInt(in.readInt());                      // MAGIC
                while (in.available() > 0) {
                    byte sep = in.readByte(); long seq = in.readLong(); long ko = in.readLong();
                    int sh = in.readInt(); int len = in.readInt(); byte[] data = new byte[len]; in.readFully(data);
                    if (seq == targetSeq) {
                        long v = new DataInputStream(new ByteArrayInputStream(data)).readLong();
                        DataOutputStream dd = new DataOutputStream(new ByteArrayOutputStream());
                        ByteArrayOutputStream nb = new ByteArrayOutputStream();
                        new DataOutputStream(nb).writeLong(v + 1);   // corrupt: value+1
                        data = nb.toByteArray(); changed = true;
                    }
                    o.writeByte(sep); o.writeLong(seq); o.writeLong(ko); o.writeInt(sh); o.writeInt(data.length); o.write(data);
                }
            }
            if (changed) { Files.write(f.toPath(), bos.toByteArray());
                System.out.println("CORRUPTED shard=" + shard + " seq=" + targetSeq + " in " + f.getName()); return; }
        }
    }
}
