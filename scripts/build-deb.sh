#!/bin/bash
set -e

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_DIR"

PACKAGE_NAME="discohack"
VERSION="1.0.0"
ARCH="all"
BUILD_DIR="deb-build/${PACKAGE_NAME}_${VERSION}_${ARCH}"

rm -rf deb-build
mkdir -p ${BUILD_DIR}/DEBIAN
mkdir -p ${BUILD_DIR}/usr/share/discohack
mkdir -p ${BUILD_DIR}/usr/bin

# Копирование кода
cp -r cache cloud fuse gui utils ${BUILD_DIR}/usr/share/discohack/
cp main.py requirements.txt ${BUILD_DIR}/usr/share/discohack/

# Запуск через /usr/bin
cat > ${BUILD_DIR}/usr/bin/discohack << 'EOF'
#!/bin/bash
cd /usr/share/discohack
python3 main.py "$@"
EOF
chmod +x ${BUILD_DIR}/usr/bin/discohack

# control
cat > ${BUILD_DIR}/DEBIAN/control << EOF
Package: ${PACKAGE_NAME}
Version: ${VERSION}
Section: utils
Priority: optional
Architecture: ${ARCH}
Maintainer: DiscoHack Team
Depends: python3, python3-pip, fuse3
Description: Cloud storage integration for Linux
EOF

# postinst
cat > ${BUILD_DIR}/DEBIAN/postinst << 'EOF'
#!/bin/bash
pip3 install -r /usr/share/discohack/requirements.txt
EOF
chmod +x ${BUILD_DIR}/DEBIAN/postinst

# Сборка
cd deb-build
dpkg-deb --build ${PACKAGE_NAME}_${VERSION}_${ARCH}
cd ..
mv deb-build/*.deb .
rm -rf deb-build

echo "Готово: $(ls *.deb)"