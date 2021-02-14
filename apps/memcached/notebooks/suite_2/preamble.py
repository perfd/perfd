import os
import sys

path_join = os.path.join
real_path = os.path.realpath

perfd_dir = real_path(path_join(os.getcwd(), "..", "..", "..", ".."))
microps_dir = path_join(perfd_dir, "thirdparty", "microps")
sys.path += [perfd_dir, microps_dir]
