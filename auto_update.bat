@echo off

cd /d D:\USUÁRIOS\092372\Documents\PYCODEBR\licitacao

echo ===============================
echo Iniciando atualizacao...
echo ===============================

:: Ativar ambiente virtual (se usar)

:: Rodar scripts Python
python extrator_bll_teixeira.py
python compra_direta.py

echo ===============================
echo Enviando para GitHub...
echo ===============================

git add .
git commit -m "Atualizacao automatica diaria"
git push

echo ===============================
echo Finalizado!
echo ===============================

pause