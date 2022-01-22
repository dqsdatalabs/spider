# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re
import dateparser
from word2number import w2n

class MySpider(Spider):
    name = 'vastgoedunie_com'
    execution_type='testing'
    country='netherlands'
    locale='nl'
    start_urls = ["https://vastgoedunie.com/woningaanbod/huur"] #LEVEL-1

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//article/div/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item)
        next_page=response.xpath("//a[@class='sys_paging next-page']/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,)
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        verhuurd = response.xpath("//title//text()[contains(.,'Verhuurd') and not(contains(.,'voorbehoud'))]").get()
        if verhuurd:
            return

        item_loader.add_value("external_link", response.url)
        f_text ="".join(response.xpath("//td[.='Type object']/following-sibling::td/text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            return

        item_loader.add_value("external_source", "Vastgoedunie_PySpider_netherlands")
        item_loader.add_xpath("title", "//title/text()")
        room = response.xpath("//td[.='Aantal kamers']/following-sibling::td/text()").get()
        if room:
            item_loader.add_value("room_count", room.split("(")[0].split())
        external_id=response.xpath("//td[.='Referentienummer']/following-sibling::td/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)
        rent=response.xpath("//td[.='Huurprijs']/following-sibling::td/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("per")[0].split("€")[1].replace(".","").replace(",","").replace("-",""))
        item_loader.add_value("currency","GBP")
        deposit=response.xpath("//td[.='Borg']/following-sibling::td/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.split("per")[0].split("€")[1].replace(".","").replace(",","").replace("-",""))

        meters =response.xpath("//td[.='Gebruiksoppervlakte wonen']/following-sibling::td/text()").get()
        if meters:
            item_loader.add_value("square_meters", meters.split("m")[0].strip())

        utilities =response.xpath("//td[.='Servicekosten']/following-sibling::td/text()").get()
        if utilities:
            uti = utilities.split("€")[1].split("p")[0].replace(",","").replace("-","").replace(".","").strip()
            item_loader.add_value("utilities",int(float(uti)))
        balcony=response.xpath("//td[.='Heeft een balkon']/following-sibling::td/text()").get()
        if balcony and balcony=="Ja":
            item_loader.add_value("balcony",True)
        desc="".join(response.xpath("//div[@class='adtext']/text()").getall())
        if desc:
            item_loader.add_value("description",desc)
        images=response.xpath("//div[@class='image']//div//img//@data-srcset").get()
        if images:
            item_loader.add_value("images",str(images).split(",")[0].replace("240w",""))


        address = response.xpath("//h1[@class='obj_address']//text()").get()
        if address:
            item_loader.add_value("address", address.split(":")[-1])
        city = response.xpath("//h1[@class='obj_address']//text()").get()
        if city:
            city=city.split(":")[-1].split(",")[-1].strip()
            city=re.search("[A-Z]+[a-z].*",city)
            item_loader.add_value("city",city.group())   
        zipcode=response.xpath("//h1[@class='obj_address']//text()").get()
        if zipcode:
            zipcode=zipcode.split(":")[-1].split(",")[-1].strip()
            item_loader.add_value("zipcode",zipcode.split(" ")[:2])

        item_loader.add_value("landlord_phone", "+31(0)20 3081600")
        item_loader.add_value("landlord_name", "Vastgoed Unie")
        item_loader.add_value("landlord_email", "info@vastgoedunie.com")  

        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "cottage" in p_type_string.lower() or "terrace" in p_type_string.lower()):
        return "house"
    else:
        return None