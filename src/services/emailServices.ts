
const nodemailer = require("nodemailer");

import { emailTemplate } from "./emailTemplate";

export class EmailService {

  private emailTemplateService: emailTemplate;
  constructor() {
    this.emailTemplateService = new emailTemplate()

  }
  async sendMailNotification(mailContent: any): Promise<any> {

    // console.log("entered  into email service --------->");
    console.log(`mail Template Content: ${JSON.stringify(mailContent)}`)
    let mailTemplateArray: any = []

    mailTemplateArray = await this.emailTemplateService.buildEmailTemplate(mailContent)
    let response: any;
    if (mailTemplateArray === undefined || mailTemplateArray == " ") {
      response = { "status": "failed", "message": "No data Received to send Mail Notification" }
      console.log("No data Received to send Mail Notification")
    }

    else {
      const transporter = nodemailer.createTransport({
        name: "www.gmail.com",
        host: "smtp.gmail.com",
        port: 587,
        secure: false, // use SSL
        ignoreTLS: false,
        auth: {
          // user: process.env.SUPPORT_EMAIL, // username for your mail server
          user: 'realtrace@realvariable.com', // username for your mail server
          pass: 'iioflqossephusqn', // password
          // pass: process.env.SUPPORT_PASSWORD, // password
        },
      });

      // Generating the transporter for each reciever

      const emailSending = mailTemplateArray?.map(async (finalTemplate: any) => {

        const mailOptions = {
          // from: process.env.SENDER_MAIL_ID,
          from: 'realtrace@realvariable.com',
          to: finalTemplate.to,
          cc: mailContent.ccEmailrecipients || [],
          subject: finalTemplate.subject,
          html: finalTemplate.htmlTemplate,
          // text: 'This is a test email sent from a Gmail account.',
        };
        console.log("entered  into email service --------->122");
        return new Promise((resolve, reject) => {
          transporter.sendMail(mailOptions, (error: any, info: any) => {
            if (error) {
              console.error('Error sending email:', error);
              reject(error);
            } else {
              console.log('Email sent:', info.response, 'sent to: ', finalTemplate.to, 'ccEmailrecipients : ', mailContent.ccEmailrecipients);
              resolve(info.response);
            }
          });
        });
      })
      try {
        await Promise.all(emailSending);
        response = { "status": "success", "message": "mail notification sent successfully" }
        // console.log('All emails sent successfully:', results);
      }
      catch (error) {
        console.error('Error sending emails:', error);
        response = { "status": "failed", "message": error }
      }
    }
    return response
  }
}
