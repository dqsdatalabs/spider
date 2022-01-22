# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import re
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
 
class MySpider(Spider):
    name = 'dolphinsquare_co_uk'
    execution_type = 'testing'
    country='united_kingdom'
    locale='en'
    external_source = "DolphinSquare_PySpider_united_kingdom"

  
    def start_requests(self):
        url = "https://www.dolphinsquare.co.uk/apartments-overview?hsLang=en-gb" # LEVEL 1
        yield Request(url=url, callback=self.jump)
    
    def jump(self, response):
        for item in response.xpath("//div[@class='apartment-type']"):
            prop_type = item.xpath(".//h3/text()").get()
            follow_url = response.urljoin(item.xpath(".//a/@href").get())
            property_type = get_p_type_string(prop_type)
            if property_type:
                yield Request(follow_url, callback=self.parse, meta={'property_type': property_type})

    def parse(self, response):
        for url in response.xpath("//div[@class='boxed_white']//h3/a/@href").getall():
            follow_url = response.urljoin(url)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta.get('property_type')})
      
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)
 
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_xpath("title","//title/text()")

        rent=response.xpath("//span[@class='price']/text()").get()
        if rent:
            rent=rent.split("£")[-1]
            item_loader.add_value("rent",int(rent)*4)
        item_loader.add_value("currency","GBP")
        deposit=response.xpath("//h5[.='TENANCY INFORMATION']/following-sibling::table/tbody/tr/td[.='Deposit']/following-sibling::td").get()
        if deposit:
            deposit=re.findall("\d+",deposit)
            item_loader.add_value("deposit",int(deposit[0])*int(rent))
        address=response.xpath("//section[@class='apartment-details']//h3//text()").get()
        if address:
            item_loader.add_value("address",address)
        zipcode=response.xpath("//section[@class='apartment-details']//h3//text()").get()
        if zipcode:
            zipcode=zipcode.split(",")[-1]
            zipcode=re.search("[A-Z]+[A-Z0-9].*",zipcode)
            if zipcode:
                item_loader.add_value("zipcode",zipcode.group())
        city=response.xpath("//section[@class='apartment-details']//h3//text()").get()
        if city:
            if len(city.split(","))==2:
                item_loader.add_value("city",city.split(",")[-1].strip().split(" ")[0])
            else:
                item_loader.add_value("city",city.split(",")[-2])
        description=response.xpath("//section[@class='apartment-details']//div[@class='apartment-boxed']//div[@class='subheader clearfix']/following-sibling::p//text()").getall()
        if description:
            item_loader.add_value("description",description)
        images=response.xpath("//img[@class='apartment-feature']/@src").getall()
        if images:
            item_loader.add_value("images",images)
        terrace=response.xpath("//ul//li[contains(.,'Gardens')]").get()
        if terrace:
            item_loader.add_value("terrace",True)
        swimming_pool=response.xpath("//ul//li[.='Swimming Pool']").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool",True)
        item_loader.add_value("landlord_name","Dolphin Square Lettings")
        item_loader.add_value("landlord_email","lettings@dolphinsquare.co.uk")
        item_loader.add_value("landlord_phone"," 020 7798 8591")
 
        
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "huis" in p_type_string.lower() or "home" in p_type_string.lower() or "cottage" in p_type_string.lower()):
        return "house"
    else:
        return None