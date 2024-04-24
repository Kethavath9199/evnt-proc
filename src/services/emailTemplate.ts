// import { format, parse } from 'date-fns';
import moment from "moment";
export class emailTemplate {

  async buildEmailTemplate(mailContent: any) {

    let obj: any = []
    let headRow: any;
    let containerNumber: any;
    let mblNumber: any;
    let awbNumber: any;
    let mailMessage: any = '';
    let ExceptionMailMessage: any = '';
    let origin = mailContent.origin || '';
    let destination = mailContent.destination || '';
    let shipper = mailContent.shipperName || '';
    let consignee = mailContent.consigneeName || '';
    let userName = mailContent.receiverName || '';
    let userRegistered = mailContent.userRegistered || false

    const buttonContent = userRegistered ? "Sign In" : "Sign Up";
    const textContent = userRegistered ? " to view in Real Trace App" : " to join Real Trace today to conveniently manage all your shipments from one centralized location";
    const realtraceAppViewLink = userRegistered ? 'https://uat-realtrace.realware.app/' : 'https://uat-realtrace.realware.app/#/signup'

    if (mailContent.notificationType == "events") {

      if (mailContent.shipmentType == "Ocean") {

        containerNumber = mailContent.shipmentNumber;
        mblNumber = mailContent.mblNumber || '';
        mailMessage = `The following events has been Updated related to your shipment with the Container Number-${containerNumber} and MBL-${mblNumber}</p>`

      }
      else {

        awbNumber = mailContent.shipmentNumber
        mailMessage = `The following events has been Updated related to your shipment with the AWB Number-${awbNumber}</p>`
      }

      const upadtedMailContent = mailContent.data.map(item => {
        let inputDate = item.date || null;
        inputDate = inputDate ? moment(inputDate).format('DD MMM YYYY') : '';

        console.log(`inputDate....:${inputDate}`)
        item.date = inputDate
        return item
      })

      // console.log(`upadtedMailContent: ${JSON.stringify(upadtedMailContent)}`)
      const tableRows = upadtedMailContent.map(item =>
        `
              <tr>
                <td>${item.eventName}</td>
                <td>${item.eventCode}</td>
                <td>${item.location}</td> 
                <td>${item.ata || '-NA-'}</td>
                <td>${item.eta || '-NA-'}</td>
                <td>${item.changeInEta || '-NA-'}</td>
              </tr>
           `).join('');

      // console.log(`tableRows: ${JSON.stringify(tableRows)}`)
      const htmlEventsTemplate = `<!DOCTYPE html>
      <html lang="en">
        <head>
          <meta charset="UTF-8" />
          <meta name="viewport" content="width=device-width, initial-scale=1" />
          <title>Shipment Arrivals</title>
          <style>
            body {
              font-family: Arial, sans-serif;
              margin: 0;
              padding: 0;
            }
            .class {
              text-decoration: none;
            }
            .ending{
              margin-left: 290px;
            }
            .container {
              max-width: 1024px;
              margin: 20px auto;
              padding: 20px;
              position: relative;
              background-color: #e3edfc;
              /* border: 2px solid #bcd3f7; */
            }
            .note {
              background-color: #007bff;
              display: inline;
              color: #fff;
              padding: 5px 10px;
              border-radius: 5px;
            }
            .container::before {
              content: "";
              position: absolute;
              top: 0;
              left: 0;
              right: 0;
              bottom: 0;
              background-image: url(cid:realTraceIconCid);
              background-repeat: no-repeat;
              background-position: center center;
              background-size: 100% 100%;
              opacity: 0.2;
              z-index: 2;
            }
            .logo {
              width: 220px;
              text-align: center;
            }
            .logo img {
              max-width: 100%;
              height: auto;
            }
            .view-real-trace {
              width: 150px;
              text-decoration: none;
              background-color: #007bff;
              color: #fff;
              border: none;
              padding: 10px 0;
              border-radius: 5px;
              text-align: center;
              cursor: pointer;
              margin: 20px 0;
            }
            table {
              width: 100%;
              table-layout: fixed;
            }
            .scrollable-table {
              max-height: 300px;
              overflow-y: auto;
              margin-bottom: 30px;
            }
            table,
            th,
            td {
              border: 1px solid #fff;
              border-collapse: collapse;
              
            }
            th,
            td {
              padding: 10px;
              word-wrap: break-word;
              text-align: left;
            }
            th {
              background-color: #bcd3f7;
            }
            .qr-note {
              width: 200px;
              margin: 15px auto;
            }
            .qr {
              width: 100px;
              margin-top: 10px;
            }
            .qr-note p {
              flex: 1;
              margin-top: 10px;
              font-size: 12px;
            }
          
            @media (max-width: 480px) {
              .container {
                padding: 10px;
              }
              .view-real-trace {
                width: 100%;
              }
              .qr-note {
                width: 100%;
              }
            }
          </style>
        </head>
        <body>
          
          <p>Hi,</p>
          <p>${mailMessage}</p>
          <center>
            <div class="container">
             
              <table class="scrollable-table">
                  <tr>
                    <th>Origin</th>
                    <th>Destination</th>
                    <th>Shipper</th>
                    <th>Consignee</th>
                  </tr>
                  <tr>
                    <td>${origin}</td>
                    <td>${destination}</td>
                    <td>${shipper}</td>
                    <td>${consignee}</td>
                  </tr>
              </table>
              <table class="scrollable-table">
                <tr>
                  <th>Event Name</th>
                  <th >Event Code</th>
                  <th>Location Name</th>
                  <th>ATA</th>
                  <th>ETA</th>
                  <th>Changed ETA </th>
                </tr>
                <tbody>
                ${tableRows}
                </tbody>
              </table>
              <p><a href= ${realtraceAppViewLink} >${buttonContent} </a> ${textContent} </p>
              <p style="font-size: 11px;">Powered by Real Variable Digital Assets Servives Pvt.Ltd (<a target="_blank" href="https://www.realvariable.com">www.realvariable.com</a>)</p>
            </div>
            
          </center>
          <div>
            <p>Regards,</p>
            <p>Team Real Trace</p>
        </div>
        </body>
      </html>
      `

      // console.log(`htmlTemplate: ${JSON.stringify(htmlEventsTemplate)}`)
      obj = [{
        "htmlTemplate": htmlEventsTemplate,
        "to": mailContent["receiverEmail"],
        "subject": `${mailContent['shipmentNumber']} - Event Updates`
      }]
      // console.log(`obj: ${JSON.stringify(obj[0])}`)
      console.log(`receiver email: ${mailContent["receiverEmail"]}`)

      return obj
    }

    else {

      if (mailContent.shipmentType == "Ocean") {
        containerNumber = mailContent.shipmentNumber
        mblNumber = mailContent.mblNumber
        ExceptionMailMessage = `The following exceptions has been Updated related to your shipment with the Container Number-${containerNumber} and MBL-${mblNumber}</p>`

      } else if (mailContent.shipmentType == "Air") {
        awbNumber = mailContent.shipmentNumber
        ExceptionMailMessage = `The following exceptions has been Updated related to your shipment with the AWB Number-${awbNumber}</p>`
      } else {
        return []
      }
      const tableRows = mailContent.data.map(item =>
        `
              <tr>
                <td>${item.exceptionTitle}</td>
                <td>${item.exceptionDescription}</td>
              </tr>
           `).join('');

      const htmlExceptionTemplate = `
      <!DOCTYPE html>
                  <html lang="en">
                    <head>
                      <meta charset="UTF-8" />
                      <meta name="viewport" content="width=device-width, initial-scale=1" />
                      <title>Shipment Event Updates</title>
                      <style>
                        body {
                          font-family: Arial, sans-serif;
                          margin: 0;
                          padding: 0;
                        }
                        .class {
                          text-decoration: none;
                        }
                        .ending{
                          margin-left: 290px;
                        }
                        .container {
                          max-width: 1024px;
                          margin: 20px auto;
                          padding: 20px;
                          position: relative;
                          background-color: #e3edfc;
                          /* border: 2px solid #bcd3f7; */
                        }
                        .note {
                          background-color: #007bff;
                          display: inline;
                          color: #fff;
                          padding: 5px 10px;
                          border-radius: 5px;
                        }
                        .container::before {
                          content: "";
                          position: absolute;
                          top: 0;
                          left: 0;
                          right: 0;
                          bottom: 0;
                          background-image: url(cid:realTraceIconCid);
                          background-repeat: no-repeat;
                          background-position: center center;
                          background-size: 100% 100%;
                          opacity: 0.2;
                          z-index: 2;
                        }
                        .logo {
                          width: 220px;
                          text-align: center;
                        }
                        .logo img {
                          max-width: 100%;
                          height: auto;
                        }
                        .view-real-trace {
                          width: 150px;
                          text-decoration: none;
                          background-color: #007bff;
                          color: #fff !important;
                          border: none;
                          padding: 10px 0;
                          border-radius: 5px;
                          text-align: center;
                          cursor: pointer;
                          margin: 20px 0;
                        }
                        table {
                          width: 100%;
                          table-layout: fixed;
                        }
                        .scrollable-table {
                          max-height: 300px;
                          overflow-y: auto;
                          margin-bottom: 30px;
                        }
                        table,
                        th,
                        td {
                          border: 1px solid #fff;
                          border-collapse: collapse;
                        }
                        th,
                        td {
                          padding: 10px;
                          text-align: left;
                        }
                        th {
                          background-color: #bcd3f7;
                        }
                        .qr-note {
                          width: 200px;
                          margin: 15px auto;
                        }
                        .qr {
                          width: 100px;
                          margin-top: 10px;
                        }
                        .qr-note p {
                          flex: 1;
                          margin-top: 10px;
                          font-size: 12px;
                        }
                      
                        @media (max-width: 480px) {
                          .container {
                            padding: 10px;
                          }
                          .view-real-trace {
                            width: 100%;
                          }
                          .qr-note {
                            width: 100%;
                          }
                        }
                      </style>
                    </head>
                    <body>
                      
                      <p>Hi ${userName}</p>
                      <p>${ExceptionMailMessage}</p>
                      <center>
                        <div class="container">
                          <table class="scrollable-table">
                              <tr>
                                <th>Origin</th>
                                <th>Destination</th>
                                <th>Shipper</th>
                                <th>Consignee</th>
                              </tr>
                              <tr>
                                <td>${origin}</td>
                                <td>${destination}</td>
                                <td>${shipper}</td>
                                <td>${consignee}</td>
                              </tr>
                          </table>
                          <table class="scrollable-table">
                            <thead>
                              <th>Exception</th>
                              <th>Description</th>
                            </thead>
                            <tbody>
                              ${tableRows}
                            </tbody>
                          </table>
                          <p><a href=${realtraceAppViewLink} > ${buttonContent} </a>  ${textContent} </p>
                          <p style="font-size: 11px;">Powered by Real Variable Digital Assets Servives Pvt.Ltd (<a target="_blank" href="https://www.realvariable.com">www.realvariable.com</a>)</p>
                        </div>
                        
                      </center>
                      <div>
                        <p>Regards,</p>
                        <p>Team Real Trace</p>
                    </div>
                    </body>
                  </html>`
      obj = [{
        "htmlTemplate": htmlExceptionTemplate,
        "to": mailContent["receiverEmail"],
        "subject": `${mailContent['shipmentNumber']} - Exceptions`
      }]
      return obj

    }
  }
}
