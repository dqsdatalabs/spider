# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from html.parser import HTMLParser
from urllib.parse import urljoin
import re
import dateparser

class MySpider(Spider):
    name = 'cattalanjohnson_com'
    execution_type='testing'
    country='france'
    locale='fr'

    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "content-type": "application/x-www-form-urlencoded",
        "origin": "https://www.cattalanjohnson.com",
        }
    
    def start_requests(self):
        start_urls = [
            {
                "url" : "https://www.cattalanjohnson.com/fr/recherche.aspx?meuble=-1"
            }
            
        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse)


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 1)
        
        seen = False
        for item in response.xpath("//div[@class='row contenus']/div[contains(@class,'liste-biens')]"):
            follow_url = response.urljoin(item.xpath(".//a[contains(.,'Plus')]/@href").get())
            prop_type = item.xpath(".//h3/text()").get()
            property_type = ""
            if "Appartement" in prop_type:
                property_type = "apartment"
            elif "Maison" in prop_type:
                property_type = "house"
            elif "Studio" in prop_type:
                property_type = "apartment"
            elif "Duplex" in prop_type:
                property_type = "apartment"
            elif "Triplex" in prop_type:
                property_type = "apartment"
            elif "immeuble" in prop_type.lower():
                property_type="house"
            if property_type != "":
                # pass
                yield Request(follow_url, callback=self.populate_item, meta={'property_type' :property_type})
            seen = True
        
        validation = response.xpath("//input[@id='__EVENTVALIDATION']/@value").get()
        viewstate = response.xpath("//input[@id='__VIEWSTATE']/@value").get()
        generator = response.xpath("//input[@id='__VIEWSTATEGENERATOR']/@value").get()
        if page%5 == 0:
            # page = 1
            target = f"ctl00$content$lvBiens$DataPager1$ctl02$ctl00"
        else:
            target = f"ctl00$content$lvBiens$DataPager1$ctl01$ctl0{page}"
        try:
            data = {
                "__EVENTTARGET": target,
                "__EVENTARGUMENT": "",
                "__LASTFOCUS": "",
                "__VIEWSTATE": f"{viewstate}",
                "__VIEWSTATEGENERATOR": f"{generator}",
                "__EVENTVALIDATION": f"{validation}",
                "prevent_autofill": "",
                "password_fake": "",
                "ctl00$recherche$ddlMeublé": "-1",
                "ctl00$recherche$txtPrixMin": "0 €",
                "txtPrixMin_ClientState": '{"enabled":true,"emptyMessage":"","validationText":"0","valueAsString":"0","minValue":0,"maxValue":99999999.99,"lastSetTextBoxValue":"0 €"}',
                "ctl00$recherche$txtPrixMax": "0 €",
                "txtPrixMax_ClientState": '{"enabled":true,"emptyMessage":"","validationText":"0","valueAsString":"0","minValue":0,"maxValue":99999999.99,"lastSetTextBoxValue":"0 €"}',
                "ctl00$recherche$txtSurfaceMin": "0 m²",
                "txtSurfaceMin_ClientState": '{"enabled":true,"emptyMessage":"","validationText":"0","valueAsString":"0","minValue":0,"maxValue":99999999,"lastSetTextBoxValue":"0 m²"}',
                "ctl00$recherche$txtSurfaceMax": "0 m²",
                "txtSurfaceMax_ClientState": '{"enabled":true,"emptyMessage":"","validationText":"0","valueAsString":"0","minValue":0,"maxValue":99999999,"lastSetTextBoxValue":"0 m²"}',
                "ctl00$recherche$txtLieu": "Location",
                "ctl00$recherche$valueLieu": "0",
                "ctl00$recherche$txtReference": "",
                "ctl00$recherche$txtMetro": "",
                "prevent_autofill": "",
                "password_fake": "",
                "ctl00$content$txtEmail": "",
                "ctl00$content$txtNom": "",
                "ctl00$content$txtPrenom": "",
                "ctl00$content$txtTelephone": "",
                "ctl00$content$ddlTri": "prix_loyer DESC",
                "ctl00$content$lvBiens$ctrl0$ctl00$HiddenField1":""+ response.xpath("//input[@name='ctl00$content$lvBiens$ctrl0$ctl00$HiddenField1']/@value").extract_first(),
                "ctl00$content$lvBiens$ctrl0$ctl00$hfId":""+ response.xpath("//input[@name='ctl00$content$lvBiens$ctrl0$ctl00$hfId']/@value").extract_first(),
                "ctl00$content$lvBiens$ctrl1$ctl00$HiddenField1":""+ response.xpath("//input[@name='ctl00$content$lvBiens$ctrl1$ctl00$HiddenField1']/@value").extract_first(),
                "ctl00$content$lvBiens$ctrl1$ctl00$hfId": ""+response.xpath("//input[@name='ctl00$content$lvBiens$ctrl1$ctl00$hfId']/@value").extract_first(),
                "ctl00$content$lvBiens$ctrl2$ctl00$HiddenField1": ""+response.xpath("//input[@name='ctl00$content$lvBiens$ctrl2$ctl00$HiddenField1']/@value").extract_first(),
                "ctl00$content$lvBiens$ctrl2$ctl00$hfId": ""+response.xpath("//input[@name='ctl00$content$lvBiens$ctrl2$ctl00$hfId']/@value").extract_first(),
                
                "ctl00$content$lvBiens$ctrl3$ctl00$HiddenField1":"" +response.xpath("//input[@name='ctl00$content$lvBiens$ctrl3$ctl00$HiddenField1']/@value").extract_first(),
                "ctl00$content$lvBiens$ctrl3$ctl00$hfId":""+ response.xpath("//input[@name='ctl00$content$lvBiens$ctrl3$ctl00$hfId']/@value").extract_first(),
                
                "ctl00$content$lvBiens$ctrl4$ctl00$HiddenField1":""+ response.xpath("//input[@name='ctl00$content$lvBiens$ctrl4$ctl00$HiddenField1']/@value").extract_first(),
                "ctl00$content$lvBiens$ctrl4$ctl00$hfId":""+ response.xpath("//input[@name='ctl00$content$lvBiens$ctrl4$ctl00$hfId']/@value").extract_first(),
                
                "ctl00$content$lvBiens$ctrl5$ctl00$HiddenField1":""+ response.xpath("//input[@name='ctl00$content$lvBiens$ctrl5$ctl00$HiddenField1']/@value").extract_first(),
                "ctl00$content$lvBiens$ctrl5$ctl00$hfId": ""+response.xpath("//input[@name='ctl00$content$lvBiens$ctrl5$ctl00$hfId']/@value").extract_first(),
                
                "ctl00$content$lvBiens$ctrl6$ctl00$HiddenField1":""+ response.xpath("//input[@name='ctl00$content$lvBiens$ctrl6$ctl00$HiddenField1']/@value").extract_first(),
                "ctl00$content$lvBiens$ctrl6$ctl00$hfId":""+ response.xpath("//input[@name='ctl00$content$lvBiens$ctrl6$ctl00$hfId']/@value").extract_first(),
                
                "ctl00$content$lvBiens$ctrl7$ctl00$HiddenField1":""+ response.xpath("//input[@name='ctl00$content$lvBiens$ctrl7$ctl00$HiddenField1']/@value").extract_first(),
                "ctl00$content$lvBiens$ctrl7$ctl00$hfId":""+ response.xpath("//input[@name='ctl00$content$lvBiens$ctrl7$ctl00$hfId']/@value").extract_first(),
                
                "ctl00$content$lvBiens$ctrl8$ctl00$HiddenField1":""+ response.xpath("//input[@name='ctl00$content$lvBiens$ctrl8$ctl00$HiddenField1']/@value").extract_first(),
                "ctl00$content$lvBiens$ctrl8$ctl00$hfId":""+ response.xpath("//input[@name='ctl00$content$lvBiens$ctrl8$ctl00$hfId']/@value").extract_first(),
                "ctl00$content$lvBiens$ctrl9$ctl00$HiddenField1":""+ response.xpath("//input[@name='ctl00$content$lvBiens$ctrl9$ctl00$HiddenField1']/@value").extract_first(),
                "ctl00$content$lvBiens$ctrl9$ctl00$hfId":""+ response.xpath("//input[@name='ctl00$content$lvBiens$ctrl9$ctl00$hfId']/@value").extract_first(),
            }
        
            # pagination = response.xpath("//a[@class='suivant']").get()
            if page < 10:
                url = "https://www.cattalanjohnson.com/fr/recherche.aspx?meuble=-1"
                yield FormRequest(url, callback=self.parse, headers=self.headers,dont_filter=True,formdata=data, meta={"page": page+1})
        except:
            pass
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        title=response.xpath("normalize-space(//div[@class='col-md-8']/h2[contains(@class,'titre')]/text())").get()
        if title:
            item_loader.add_value("title", title)
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Cattalanjohnson_PySpider_"+ self.country + "_" + self.locale)
        
        latitude_longitude = response.xpath("//script[contains(.,'LatLng')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(',')[0].strip()
            longitude = latitude_longitude.split('LatLng(')[1].split(',')[1].split(')')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        address = title.split("Appartement")[-1].split("Maison")[-1].replace("Meublée","").replace("Meublé","").strip().split(" ")[0]
        if address:
            item_loader.add_value("address", address)
            if not address.isdigit():
                item_loader.add_value("city", address)
           
        bathroom_count = response.xpath("//div[@id='content_divNbSallesBain']/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(" ")[0])
        
        square_meters = response.xpath("//div[@id='content_divSurface']/text()").get()
        if square_meters:
            square_meters = str(int(float(square_meters.split(':')[1].split('m')[0].strip())))
            item_loader.add_value("square_meters", square_meters)


        room_count = response.xpath("//div[@id='content_divNbChambre' or @id='content_divNbPiece' ]/text()").get()
        if room_count:
            room_count = room_count.strip().split(' ')[0]
            item_loader.add_value("room_count", room_count)

        rent = response.xpath("//p[@class='prix']/text()").get()
        if rent:
            rent = rent.split('€')[0].strip().replace(' ', '').replace('\xa0', '')
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", 'EUR')

        zipcode = response.xpath("//div[@id='content_divPlansTransport']/following-sibling::text()[contains(.,'LOCATION')]").get()
        code = ""
        if zipcode:
            zipcode = zipcode.replace("(","").replace(")","").split(" ")
            if zipcode[-1].isdigit():
                code = zipcode[-1].strip()
            elif zipcode[-2].isdigit():
                code = zipcode[-2].strip()

            if code and code !="3":
                item_loader.add_value("zipcode", code)
                
        external_id = response.xpath("//h3[@class='text-primary']/text()").get()
        if external_id:
            external_id = external_id.strip().strip('Ref.').strip()
            item_loader.add_value("external_id", external_id)

        description = response.xpath(
            "//div[@id='content_divPlansTransport']/following-sibling::text() | //div[@class='col-md-6 col-sm-6']/text()").getall()
        desc_html = ''      
        if description:
            for d in description:
                desc_html += d.strip() + ' '
            desc_html = desc_html.replace('\xa0', '').replace("\u20ac","")
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", filt.text)

        if "de charge" in desc_html:
            utilities = desc_html.split("de charges")[0].strip().split(" ")[-1]
            item_loader.add_value("utilities", utilities.replace("(",""))
        elif "charge" in desc_html.lower():
            utilities = desc_html.lower().split("charge")[0]
            if "+" in utilities:
               utilities = utilities.split("+")[-1].replace("de","").strip()
            elif "et" in utilities:
               utilities = utilities.split("et")[-1].replace("de","").strip()
            if utilities.isdigit():
                item_loader.add_value("utilities", utilities)
        
        available_date = False
        if "DISPONIBLE LE" in desc_html:
            available_date = desc_html.split("DISPONIBLE LE")[1].strip().split(" ")[0]
        elif "DISPONIBLE" in desc_html:
            available_date = desc_html.split("DISPONIBLE")[1].split("AUTRES")[0].strip()
        if available_date:
            try:
                date_parsed = dateparser.parse(
                    available_date, date_formats=["%d/%m/%Y"]
                )
            except:
                date_parsed = False
                
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")    
                item_loader.add_value("available_date", date2)
            
        
        images = [urljoin('https://www.cattalanjohnson.com/', x) for x in response.xpath("//div[@class='container-miniature no-print']/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))

        furnished = response.xpath("//div[@id='content_divMobilier']/text()").get()
        if furnished:
            if furnished.strip().lower() == 'non':
                furnished = False
            else:
                furnished = True
            if type(furnished) == bool:
                item_loader.add_value("furnished", furnished)
        elif response.xpath("//h2[contains(.,'Meublé')]/text()").get():
            item_loader.add_value("furnished", True)

        floor = response.xpath("//div[@id='content_divEtage']/text()").get()
        if floor:
            floor = floor.split(':')[1].strip()
            item_loader.add_value("floor", floor)

        parking = response.xpath("//div[@id='content_divNbParking']/text()").get()
        if parking:
            parking = True
            item_loader.add_value("parking", parking)

        elevator = response.xpath("//div[@id='content_divAscenceur']/text()").get()
        if elevator:
            elevator = True
            item_loader.add_value("elevator", elevator)

        balcony = response.xpath("//div[@id='content_divNbBalcons']/text()").get()
        if balcony:
            balcony = True
            item_loader.add_value("balcony", balcony)
        
        terrace = response.xpath("//div[@id='content_divTerrasse']/text()").get()
        if terrace:
            terrace = True
            item_loader.add_value("terrace", terrace)

        swimming_pool = response.xpath("//div[@id='content_divPiscine']/text()").get()
        if swimming_pool:
            swimming_pool = True
            item_loader.add_value("swimming_pool", swimming_pool)

        washing_machine = response.xpath("//div[@id='content_divLaveLinge']/text()").get()
        if washing_machine:
            washing_machine = True
            item_loader.add_value("washing_machine", washing_machine)

        dishwasher = response.xpath("//div[@id='content_DivLaveVaisselle']/text()").get()
        if dishwasher:
            dishwasher = True
            item_loader.add_value("dishwasher", dishwasher)

        item_loader.add_value("landlord_name", "CATTALAN JOHNSON IMMOBILIER")
        
        landlord_phone = response.xpath("//span[@class='glyphicons glyphicons-earphone']/following-sibling::text()").get()
        if landlord_phone:
            landlord_phone = landlord_phone.strip()
            item_loader.add_value("landlord_phone", landlord_phone)

        landlord_email = response.xpath("//span[@class='glyphicons glyphicons-envelope']/following-sibling::text()").get()
        if landlord_email:
            landlord_email = landlord_email.strip()
            item_loader.add_value("landlord_email", landlord_email)
        
        yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data