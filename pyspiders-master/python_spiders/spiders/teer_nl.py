# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from datetime import date
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re


class MySpider(Spider):
    name = 'teer_nl'
    execution_type = 'testing' 
    country = 'netherlands'
    locale = 'nl' # LEVEL 1

    def start_requests(self):
        url = "https://teer.nl/verhuurmakelaar-noord-holland/huuraanbod/?location=1"
        yield Request( url,callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[@class='grid-item']"):
            url = response.urljoin(item.xpath("./@href").extract_first())
            yield Request(url, callback=self.populate_item)
            

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
       

        item_loader.add_value("external_source", "Teer_PySpider_" + self.country + "_" + self.locale)

        status = "".join(response.xpath("//div[div[.='Status:']]/div[2]/text()").getall())
        if status and "verhuurd" in status.lower() or "te huur" in status.lower():
            return
        status1=response.xpath("//div[.='Status:']/following-sibling::div/p/text()").get()
        if status1 and "verhuurd" in status1.lower():
            return 
        item_loader.add_xpath("title","//title/text()")
        id=response.url
        if id:
            item_loader.add_value("external_id",id.split("id=")[-1])
        desc = " ".join(response.xpath("//div[@id='read-more-content']//p//text()").extract())
        if desc:
            item_loader.add_value("description",desc.strip())
            
            if "beschikbaar per" in desc.lower():
                available_date = desc.lower().split('beschikbaar per')[1].split(".")[0].strip("a").strip()
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
                    
            if "waarborgsom" in desc.lower():
                deposit = desc.lower().split('waarborgsom')[1]
                if "maanden" in deposit:
                    deposit = deposit.split("maanden")[0].strip().split(" ")[-1]
                elif "maandhuur" in deposit:
                    deposit = deposit.split("maandhuur")[0].replace("x","").strip().split(" ")[-1]

        address = " ".join(response.xpath("concat(//h1[@class='street']//text(), ' ',//p[@class='city']//text())").extract())
        if address:
            item_loader.add_value("address",address)
        zipcode=response.xpath("//p[@class='city']//text()").get()
        if zipcode:
            if "(" in zipcode:
                item_loader.add_value("zipcode",zipcode.split("(")[-1].split(")")[0].upper())

        item_loader.add_value("external_link", response.url)
        latitude=response.xpath("//script[contains(.,'lng')]/text()").get()
        if latitude:
            lat=latitude.split("position: {")[-1].split("lat")[-1].split(",")[0]
            item_loader.add_value("latitude",lat)
        longitude=response.xpath("//script[contains(.,'lng')]/text()").get()
        if longitude:
            lng=latitude.split("position: {")[-1].split("lng")[-1].split("}")[0]
            item_loader.add_value("longitude",lng)

        property_type = " ".join(response.xpath("//div[@id='read-more-content']//p//text()").extract())
        if property_type:
            if get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))
            else: return
        # else: return
        
        price = "".join(response.xpath("//div[div[.='Huurprijs']]/div[2]/p/text()").extract())
        if price:
            price = price.split("€")[1].split(",")[0].strip().replace(".","")
            item_loader.add_value("rent", price)

            # if deposit.isdigit():
            #     item_loader.add_value("deposit", int(deposit)*int(float(price)))
            # elif "twee" in deposit:
            #     item_loader.add_value("deposit", 2*int(float(price)))
                
        item_loader.add_value("currency", "EUR")
        item_loader.add_xpath("city", "//p[@class='city']//text()")

        utilities = response.xpath("//div[.='Servicekosten:']/following-sibling::div/p/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split(",")[0].replace("€","").strip())
            

        square =response.xpath("//div[.='Woonoppervlakte:']/following-sibling::div/p/text()").get()
        if square:
            item_loader.add_value("square_meters", square.split("m")[0].strip())


        images = [response.urljoin(x)for x in response.xpath("//div[@class='img-container']/img/@src").extract()]
        if images:
                item_loader.add_value("images", images)


        terrace = "".join(response.xpath("//span[span[. ='Balkon']]/span[@class='kenmerkValue']/text() | //span[span[. ='Balkon']]/span[@class='kenmerkValue']/ya-tr-span/text()").extract())
        if terrace:
            if "Ja" in terrace:
                item_loader.add_value("balcony", True)


        terrace = "".join(response.xpath("//span[span[. ='Parkeerfaciliteiten']]/span[@class='kenmerkValue']/text() | //span[span[. ='Parkeerfaciliteiten']]/span[@class='kenmerkValue']/ya-tr-span/text()").extract())
        if terrace:
                item_loader.add_value("parking", True)

        terrace = "".join(response.xpath("//span[span[. ='Voorzieningen']]/span[@class='kenmerkValue']/text()[contains(.,'Lift')] | //span[span[. ='Voorzieningen']]/span[@class='kenmerkValue']/ya-tr-span/text()[contains(.,'Lift')]  ").extract()).strip()
        if terrace:
            item_loader.add_value("elevator", True)

        terrace = "".join(response.xpath("//span[span[. ='Bijzonderheden']]/span[@class='kenmerkValue']/text()[contains(.,'Gestoffeerd')]| //span[span[. ='Bijzonderheden']]/span[@class='kenmerkValue']/ya-tr-span/text()[contains(.,'Gestoffeerd')]").extract()).strip()
        if terrace:
            item_loader.add_value("furnished", True)

        item_loader.add_value("landlord_phone", "0251-745630")
        item_loader.add_value("landlord_email", "castricum@teer.nl")
        item_loader.add_value("landlord_name", "Teer Makelaars Castricum CASTRICUM")

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "home" in p_type_string.lower() or "woning" in p_type_string.lower()):
        return "house"
    else:
        return None