# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json,re

class MySpider(Spider):
    name = 'gabinohome_com'
    execution_type='testing' 
    country='turkey'
    locale='tr'
    external_source = "Gabinohome_PySpider_turkey"
    start_urls = ["https://www.gabinohome.com/es/alquiler-habitaciones-istanbul-c1r5127.html"]
    custom_settings = {"HTTPCACHE_ENABLED":False}
    
    # 1. FOLLOWING
    def parse(self, response):
        # print("parse girdikkk")
        for item in response.xpath("//section[@class='list-details']"):

            follow_url = response.urljoin(item.xpath(".//h2//@href").get())
            # print(follow_url)

            # property_type = "".join(item.xpath("//ul[@class='list-services']/li/text()").getall())
            # if get_p_type_string(property_type):
            #     print("request gittiiiiiii")
            yield Request(follow_url, callback=self.populate_item)
        
        next_page = response.xpath("//link[@rel='next']/@href").get()
        if next_page:
            yield Request(next_page, callback=self.parse)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("title", "//h1/text()")
        # item_loader.add_xpath("room_count", "//li[contains(.,'bedroom')]/strong/text()")
        item_loader.add_xpath("bathroom_count", "//li[contains(.,'bathroom')]/strong/text()")
        if not item_loader.get_collected_values("room_count") and (response.meta.get('property_type') == "room" or response.meta.get('property_type') == "studio"):
            item_loader.add_value("room_count", "1")
        # room_check=item_loader.get_output_value("room_count")
        # if not room_check:
        #     room=response.xpath("//div[@id='item-desc']/p/text()").get()
        #     if room:
        #         item_loader.add_value("room_count",room.split(" ")[0])

        item_loader.add_value("room_count","1")

        # rent = "".join(response.xpath("//span[@itemprop='priceRange'][contains(.,'/month')]//text()").getall())
        # rent_week = "".join(response.xpath("//span[@itemprop='priceRange'][contains(.,'/week')]/text()").getall())
        # rent_day = "".join(response.xpath("//span[@itemprop='priceRange'][contains(.,'/day')]/text()").getall())
        # rent_currency = "".join(response.xpath("//span[@itemprop='priceRange'][contains(.,'/week')]/span[@class='price-currency']/text()").getall())
        # rent_currencyday = "".join(response.xpath("//span[@itemprop='priceRange'][contains(.,'/day')]/span[@class='price-currency']/text()").getall())
        # if rent:
        #     item_loader.add_value("rent_string", rent.replace(",","").split("/")[0])  
        # elif rent_week:
        #     item_loader.add_value("rent_string", str(int(rent_week.replace(",","").split("/")[0])*4) + rent_currency)  
        # elif rent_day:
        #     item_loader.add_value("rent_string", str(int(rent_day.replace(",","").split("/")[0])*30) + rent_currencyday)  

        rent = response.xpath("//span[text()='/mes']/parent::span[@itemprop='priceRange']/text()").get()
        if rent:
            rent = rent.replace(".","")
            if int(rent)<100000:
                item_loader.add_value("rent",rent)
            else:
                rent = rent[:3]
                item_loader.add_value("rent",rent)
            item_loader.add_value("rent",rent.replace(".","").strip())
        else:
            rent = response.xpath("//span[text()='/semana']/parent::span[@itemprop='priceRange']/text()").get()
            if rent:
                rent = rent.replace(".","")
                if int(rent)<100000:
                    item_loader.add_value("rent",int(rent)*4)
                else:
                    rent = rent[:3]
                    item_loader.add_value("rent",int(rent)*4)
                    
        external_id = response.xpath("//div[@id='item-ref']/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[-1].strip())       

        address = "".join(response.xpath("//div[@id='item-address-header']/text()[normalize-space()]").getall())
        if address:
            item_loader.add_value("address", address.strip())
            if  address.split(",")[-2].strip().isdigit():
                item_loader.add_value("city", address.split(",")[-3].strip())
                item_loader.add_value("zipcode", address.split(",")[-2].strip())
            else:
                item_loader.add_value("city", address.split(",")[-2].strip())
 
        square = response.xpath("//li[contains(.,'m²')]/strong/text()").extract_first()
        if square:       
            item_loader.add_value("square_meters", square)

        furnished = "".join(response.xpath("//li/text()[contains(.,'Furnished') or contains(.,'furnished')]").getall())
        if furnished:
            if "unfurnished" in furnished.lower() :
                item_loader.add_value("furnished",False)
            else:
                item_loader.add_value("furnished",True)

        pets_allowed = "".join(response.xpath("//li/text()[contains(.,' pets') or contains(.,'Pets')]").getall())
        if pets_allowed:
            if "no " in pets_allowed.lower() :
                item_loader.add_value("pets_allowed",False)
            else:
                item_loader.add_value("pets_allowed",True)

        parking = response.xpath("//li/text()[contains(.,'parking') or contains(.,'Parking')]").get()
        if parking:
            item_loader.add_value("parking",True)

        terrace = response.xpath("//li/text()[contains(.,'Terrace') or contains(.,'terrace')]").get()
        if terrace:
            item_loader.add_value("terrace",True)

        desc = "".join(response.xpath("//div[@id='item-desc']//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
        

        images = [response.urljoin(x) for x in response.xpath("//div[@id='item-main-photo']//li//a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        name = response.xpath("//div[@id='client-name']//text()").get()
        if name:
            item_loader.add_value("landlord_name", name.strip())
        else:
            item_loader.add_value("landlord_name", "Gabinohome S.L")
        landlord_phone = response.xpath("//div[@id='client-tel']//text()[.!='']").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone.strip())
        else:
            item_loader.add_xpath("landlord_phone", "//span[@class='bt-tel']/@data-num")


        position = response.xpath("//script[contains(text(),'maps/search')]/text()").get()
        if position:
            lat = re.search("api=1&query=([\d.]+)", position)
            if lat:
                item_loader.add_value("latitude",lat.group(1))
            long = re.search("api=1&query=([\d.]+,([\d.]+))", position)
            if long:
                item_loader.add_value("longitude", long.group(2))
        item_loader.add_value("landlord_email","EMAIL.soporte@gabinohome.com ")
        currency = response.xpath("//span[@class='price-currency']/text()").get()
        if currency:
            if "£" in currency:
                item_loader.add_value("currency","GBP")
            elif "€" in currency:
                item_loader.add_value("currency","EUR")
            elif "$" in currency:
            
                item_loader.add_value("currency","USD")
            else:
                item_loader.add_value("currency","TRY")

        item_loader.add_value("property_type","room")



        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "flatshare" in p_type_string.lower():
        return "room"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "huis" in p_type_string.lower() or "home" in p_type_string.lower() or "cottage" in p_type_string.lower()):
        return "house"
    else:
        return None
      