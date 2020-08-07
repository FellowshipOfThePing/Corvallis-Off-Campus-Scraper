
var stats = JSON.parse(document.getElementById('stats').textContent);


var ctx = document.getElementById("myChart").getContext("2d");
var myChart = new Chart(ctx, {
  type: "bar",
  data: {
    labels: ["insufficient_data", "wrong_city", "unavailable", "error"],
    datasets: [
      {
        label: "Skipped By Type",
        data: [
            stats["skipped_by_type"]["insufficient_data"],
            stats["skipped_by_type"]["wrong_city"],
            stats["skipped_by_type"]["unavailable"],
            stats["skipped_by_type"]["error"]],
        backgroundColor: [
          "rgba(255, 99, 132, 0.2)",
          "rgba(54, 162, 235, 0.2)",
          "rgba(255, 206, 86, 0.2)",
          "rgba(75, 192, 192, 0.2)",
        ],
        borderColor: [
          "rgba(255, 99, 132, 1)",
          "rgba(54, 162, 235, 1)",
          "rgba(255, 206, 86, 1)",
          "rgba(75, 192, 192, 1)",
        ],
        borderWidth: 1,
      },
    ],
  },
  options: {
    scales: {
      yAxes: [
        {
          ticks: {
            beginAtZero: true,
          },
        },
      ],
    },
  },
});
