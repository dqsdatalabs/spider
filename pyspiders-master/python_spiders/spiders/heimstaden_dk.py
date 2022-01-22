# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider, item
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re
import dateparser

class MySpider(Spider):
    name = 'heimstaden_dk'
    execution_type = 'testing'
    country = 'denmark'
    locale ='da'
    start_urls = ['https://www.heimstaden.dk/ledige-lejeboliger'] # LEVEL 1
    external_source="Heimstaden_PySpider_denmark"
    custom_settings = {
        "PROXY_TR_ON" : True,
        "HTTPCACHE_ENABLED": False,
        "HTTPERROR_ALLOWED_CODES": [301,302,403,503]
    }
 
    headers={
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
        "cache-control": "max-age=0",
        "cookie": "CookieInformationConsent=%7B%22website_uuid%22%3A%22b43c2328-b6f3-4eae-a730-472b1a7c52ee%22%2C%22timestamp%22%3A%222021-10-04T12%3A05%3A02.630Z%22%2C%22consent_url%22%3A%22https%3A%2F%2Fwww.heimstaden.dk%2Fledige-lejeboliger%2F%22%2C%22consent_website%22%3A%22heimstaden.dk%22%2C%22consent_domain%22%3A%22www.heimstaden.dk%22%2C%22user_uid%22%3A%22ed125ab1-9131-49bb-a5e6-a14ce3d80696%22%2C%22consents_approved%22%3A%5B%22cookie_cat_necessary%22%2C%22cookie_cat_functional%22%2C%22cookie_cat_statistic%22%2C%22cookie_cat_marketing%22%2C%22cookie_cat_unclassified%22%5D%2C%22consents_denied%22%3A%5B%5D%2C%22user_agent%22%3A%22Mozilla%2F5.0%20%28Windows%20NT%2010.0%3B%20Win64%3B%20x64%29%20AppleWebKit%2F537.36%20%28KHTML%2C%20like%20Gecko%29%20Chrome%2F94.0.4606.61%20Safari%2F537.36%22%7D; _ga=GA1.2.640389398.1633349087; _gcl_au=1.1.1296483764.1633349103; _fbp=fb.1.1633349103185.1810050473; hubspotutk=540377f5ccf4a25999f3b60c6daa4c86; _gid=GA1.2.215644526.1634816552; __hstc=119416726.540377f5ccf4a25999f3b60c6daa4c86.1633349105158.1633945136432.1634816565459.3; __hssrc=1; __hssc=119416726.5.1634816565459",
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.61 Mobile Safari/537.36",
    }

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='rentals']/a"):          
   
          follow_url = response.urljoin(item.xpath("./@href").get())
          type = item.xpath(".//span[@class='rental-type']/text()").get()  
          zipcode = item.xpath(".//h3/span[@class='rental-title-zip']/text()").get()  
          city = item.xpath(".//h3/text()[last()]").get()  
          property_type = ""
          if type:
            # print(type)   
            if get_p_type_string(type):
                property_type = get_p_type_string(type)
           
          yield Request(follow_url,callback=self.populate_item,meta = {"property_type":property_type,"zipcode":zipcode,"city":city})


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("external_id", "substring-after(//span/text()[contains(.,'Lejemålsnr. ')],'Lejemålsnr. ')")
        item_loader.add_xpath("title", "//div[@class='inner']//h2/text()")

        property_type =response.meta.get("property_type")
        if not property_type:
            return
        item_loader.add_value("property_type", property_type)

        zipcode =response.meta.get("zipcode")
        city =response.meta.get("city")
        item_loader.add_xpath("address", "//div[@class='inner']//h2/text()")
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode) 

        price = response.xpath("//div[label[.='Husleje pr. md.']]/span/text()").get()
        if price:
            rent = price.replace(".","").split(",")[0]
            item_loader.add_value("rent_string", rent.strip())
        item_loader.add_value("currency","DKK")
        
        item_loader.add_xpath("room_count", "//div[label[.='Værelser']]/span/text()")
        
        meters = response.xpath("//div[label[.='Boligareal']]/span/text()").get()
        if meters:
            item_loader.add_value("square_meters",meters.split("m")[0])

        available_date= response.xpath("//div[label[.='Overtagelse']]/span/text()[.!='Ledig nu']").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.replace("Ledig fra","").strip(), date_formats=["%m-%d-%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))

        description = " ".join(response.xpath("//div[@class='col-md-8 rental-description']/div/p//text()").getall())
        if description:
            item_loader.add_value("description", description)

        pets_allowed =  response.xpath("//span[label[.='Dyr tilladt:']]/text()").get()
        if pets_allowed:
            if "Nej" in pets_allowed:
                item_loader.add_value("pets_allowed", False)
            else:
                item_loader.add_value("pets_allowed", True)   

        parking =  response.xpath("//span[label[.='Parkering:']]/text()").get()
        if parking:
            if "Nej" in parking:
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)   
        elevator =  response.xpath("//span[label[.='Elevator:']]/text()").get()
        if elevator:
            if "Nej" in elevator:
                item_loader.add_value("elevator", False)
            else:
                item_loader.add_value("elevator", True)   
        washing_machine =  response.xpath("//span[label]/text()[contains(.,'vaskemaskine')]").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)   

        item_loader.add_xpath("energy_label", "//span[label[.='Energimærke:']]/text()")
        deposit=response.xpath("//div[label[.='Depositum']]/span/text()").get()
        if deposit:
            deposit = deposit.replace(".","").split(",")[0]
            item_loader.add_value("deposit", deposit)

        latitude=response.xpath("//div[@id='map']/@data-lat").get()
        if latitude:
            item_loader.add_value("latitude", latitude)
        longitude=response.xpath("//div[@id='map']/@data-lng").get()
        if longitude:
            item_loader.add_value("longitude", longitude)

        images = [x for x in response.xpath("//div[@class='swiper-slide slide-image']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        floor_plan_images = [x for x in response.xpath("//div[@class='swiper-slide slide-plan contain']//img/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        item_loader.add_xpath("landlord_name", "//div[@class='contact sidebar-contact']/div/strong/text()")       
        item_loader.add_xpath("landlord_phone", "//div[@class='contact sidebar-contact']//span[@class='phone']/a/text()")       
    
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
   
    if p_type_string and ("lejlighed" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    if p_type_string and ("rækkehus" in p_type_string.lower() or "tvillingehus" in p_type_string.lower() or "parcelhus" in p_type_string.lower() or "dobbelthus" in p_type_string.lower() or "pyramidehus" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None
