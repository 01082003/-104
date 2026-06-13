
import sys
import os
import shutil
import struct
from pathlib import Path
import threading
from multiprocessing import Manager, Process

class Sort:
    def __init__(self,input_file, chunk_size):
        self.input_file = Path(input_file)
        self.chunk_size = chunk_size
        self.work_dir = Path(f"temp_{input_file}")

    def _sort_and_save(self,chunk_id, numbers, work_dir, queue):
        numbers.sort()
        file_path = work_dir / f"chunk_{chunk_id}.dat"
        with open(file_path, 'wb') as out:  # 'wb' = write binary (запись байтов)
            for num in numbers:
                out.write(struct.pack('<I', num))
        queue.put(file_path)

    def sort(self):
        if not self.work_dir.exists():
            self.work_dir.mkdir()
        result_queue = Manager().Queue()
        processes = []
        chunk_id = 0
        with open(self.input_file, 'rb') as f: #'rb' — read binary (читать как байты)
            while True:
                numbers = []
                for _ in range(self.chunk_size):
                    data = f.read(4)
                    if not data:
                        break
                    numbers.append(struct.unpack('<I', data)[0])

                if not numbers:
                    break

                p = Process(target=self._sort_and_save, args=(chunk_id, numbers, self.work_dir, result_queue)) # процесс т к нет взаимодействия
                processes.append(p) # список процессов
                p.start()
                chunk_id += 1
            for p in processes:
                p.join()

            sorted_files = []
            for _ in range(chunk_id):
                sorted_files.append(result_queue.get())

            # СЛИЯНИЕ
            while len(sorted_files) > 1:
                pairs = []
                next_files = []
                for i in range(0, len(sorted_files), 2):
                    if i + 1 < len(sorted_files):
                        out_file = self.work_dir / f"merge_{i}.dat"
                        pairs.append((sorted_files[i], sorted_files[i+1], out_file))
                        next_files.append(out_file)
                    else:
                        next_files.append(sorted_files[i])
                def merge_two(file1, file2, output):
                    with open(file1, 'rb') as f1, open(file2, 'rb') as f2, open(output, 'wb') as out:
                        d1 = f1.read(4)
                        d2 = f2.read(4)
                        n1 = struct.unpack('<I', d1)[0] if d1 else None
                        n2 = struct.unpack('<I', d2)[0] if d2 else None
                        while n1 is not None and n2 is not None:
                            if n1 <= n2:
                                out.write(struct.pack('<I', n1))
                                d1 = f1.read(4)
                                n1 = struct.unpack('<I', d1)[0] if d1 else None
                            else:
                                out.write(struct.pack('<I', n2))
                                d2 = f2.read(4)
                                n2 = struct.unpack('<I', d2)[0] if d2 else None
                        while n1 is not None:
                            out.write(struct.pack('<I', n1))
                            d1 = f1.read(4)
                            n1 = struct.unpack('<I', d1)[0] if d1 else None

                        while n2 is not None:
                            out.write(struct.pack('<I', n2))
                            d2 = f2.read(4)
                            n2 = struct.unpack('<I', d2)[0] if d2 else None
                threads = []
                for file1, file2, out_file in pairs:
                    t = threading.Thread(target=merge_two, args=(file1, file2, out_file)) # потоки т к в основном читаем-записываем файлы
                    threads.append(t)
                    t.start()

                for t in threads:
                    t.join()

                for f in sorted_files:
                    if f not in next_files and f.exists():
                        try:
                            f.unlink()
                        except:
                            pass

                sorted_files = next_files
            final_file = self.input_file.parent / f"{self.input_file.stem}_sorted.bin"
            shutil.move(str(sorted_files[0]), str(final_file)) #Перемещает единственный оставшийся файл в финальное имя.
            shutil.rmtree(self.work_dir) # Удаляет временную папку

            print(f"Готово! Результат: {final_file}")
            return final_file


if __name__ == "__main__":


    if len(sys.argv) != 3:
        sys.exit(1)

    input_file = sys.argv[1]
    chunk_size = int(sys.argv[2])

    if not os.path.exists(input_file):
        print(f"Ошибка: файл {input_file} не найден!")
        sys.exit(1)

    sorter = Sort(input_file, chunk_size)
    sorter.sort()
