from . import cmd_out


def get_tags(image) -> list:
    raw = cmd_out(f"wget -q https://registry.hub.docker.com/v1/repositories/{image}/tags -O -  "
                  "| sed -e 's/[][]//g' -e 's/\"//g' -e 's/ //g' | tr '}' '\n'  | awk -F: '{print $3}'")
    return raw.decode().split()


if __name__ == '__main__':
    print(get_tags("memcached"))
