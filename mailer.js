const nodemailer = require("nodemailer");
const path = require("path");

const transporter = nodemailer.createTransport({
  host: "10.36.66.46",       
  port: 25,     
  secure: false,
  tls: {
    rejectUnauthorized: false,
  },
});

const TO_EMAILS  = ["tsogtbaatar.e@mobicom.mn",];
const CC_EMAILS  = [];  
const ATTACHMENT = path.resolve(__dirname, "output_analysis_da2_2.xlsx");
const today      = new Date().toISOString().slice(0, 10);

async function sendMail() {
  try {
    await transporter.sendMail({
      from: "bumdari.b@mobicom.mn",
      to: TO_EMAILS,
      cc: CC_EMAILS,
      subject: `Баттерийн тайлан — ${today}`,
      html: `
        <p>Сайн байна уу,</p>
        <p><b>${today}</b>-ны өдрийн сайтын баттерийн анализ тайланг хавсаргав.</p>
        <ul>
          <li>Баттерийн ерөнхий байдал (critical / degrading / stable)</li>
          <li>Drop percent болон drop level</li>
          <li>Status & Forecast — цаашдын таамаглал</li>
        </ul>
        <p>Хүндэтгэлтэй,<br>TTST</p>
      `,
      attachments: [
        {
          filename: `battery_report_${today}.xlsx`,
          path: ATTACHMENT,
        },
      ],
    });
    console.log("✓ Имэйл амжилттай илгээгдлээ!");
  } catch (error) {
    console.error("✗ Имэйл илгээхэд алдаа гарлаа:", error);
    process.exit(1);
  }
}

sendMail();