// display current filename in upload input
document.addEventListener('DOMContentLoaded', () => {
  [...document.querySelectorAll('.file input')].forEach((input) => {
    input.nextElementSibling.setAttribute('data-filename', 'Choose file...');
    input.addEventListener('change', function (e) {
      this.nextElementSibling.querySelector('.file-label').innerText = this.files[0].name;
    });
  });
});
