// display current filename in upload input
addEventListener(
  'DOMContentLoaded',
  () => {
    // timeout to set filenames on navigating back
    setTimeout(() => {
      document.querySelectorAll('.file input').forEach((input) => {
        const label = input.nextElementSibling.querySelector('.file-label');
        const defaultVal = label.innerText;
        const filename = input.files[0]?.name;
        if (filename) label.innerText = filename;
        input.addEventListener('change', function () {
          label.innerText = this.files[0]?.name ?? defaultVal;
        });
      });
    }, 1);
  },
  true,
);
