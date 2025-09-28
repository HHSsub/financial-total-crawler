if (Test-Path 'C:\Users\User\Downloads\Downloads\financial-total-crawler\analysis\analysis.lock') {
    exit 0
}
New-Item -ItemType File -Path 'C:\Users\User\Downloads\Downloads\financial-total-crawler\analysis\analysis.lock' -Force | Out-Null

try {
    & 'C:\Users\User\miniconda3\Scripts\activate.bat' finance
    & 'C:\Users\User\miniconda3\envs\finance\python.exe' 'C:\Users\User\Downloads\Downloads\financial-total-crawler\analysis\analysis.py'
}
finally {
    Remove-Item 'C:\Users\User\Downloads\Downloads\financial-total-crawler\analysis\analysis.lock'
}
