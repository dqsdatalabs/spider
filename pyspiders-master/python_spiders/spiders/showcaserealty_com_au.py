# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

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
    name = 'showcaserealty_com_au'  
    execution_type='testing'
    country='australia'
    locale='en'     
    start_urls = ["https://www.showcaserealty.com.au/search?searchKeyword=ID%2C+Suburb+or+Postcode&disposalMethod=rent&city=&priceMinimum=0&priceMaximum=0&bedrooms=&bathrooms=&carspaces=&search=&_fn=quicksearch"]

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[contains(@class,'content margin-bottom-30')]"):
            status = item.xpath(".//div[@class='statusBackground']/h5/text()").get()
            if status and ("leased" in status.lower()):
                continue
            follow_url = item.xpath("./a/@href").get()
            yield Request(response.urljoin(follow_url), callback=self.populate_item)
        
        next_page = response.xpath("//a[.='NEXT']/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
            )


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        deposit_taken = response.xpath("//h2[@class='text-left']/span/text()").extract_first()
        if "deposit" in deposit_taken.lower():
            return
        item_loader.add_value("external_source", "Showcaserealty_Com_PySpider_australia") 
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title", "//title/text()")
        square_meters = response.xpath("//h3/text()[contains(.,'sqm')]").get()
        if square_meters:            
            item_loader.add_value("square_meters", square_meters.split("sqm")[0].strip().split(" ")[-1])

        p_type = "".join(response.xpath("//div/h3/text()").getall())
        if get_p_type_string(p_type):
            p_type = get_p_type_string(p_type)
            item_loader.add_value("property_type", p_type)
        else:
            p_type = "".join(response.xpath("//h3/../p//text()").getall())
            if get_p_type_string(p_type):
                p_type = get_p_type_string(p_type)
                item_loader.add_value("property_type", p_type)
            else:
                return
        rent = "".join(response.xpath("//h2[@class='text-left']/span/text()[not(contains(.,'Ask Agent') or contains(.,'Rent Negotiable'))]").getall())
        if rent:
            price = rent.split("$")[1].strip().split(" ")[0].replace("p/w","").replace(",","").replace("pw","").replace("/week","").strip()
            item_loader.add_value("rent", int(float(price))*4)
        item_loader.add_value("currency", "AUD")
        item_loader.add_xpath("room_count", "//li[contains(@class,'list-group-item')]/b[.='Bedrooms']/following-sibling::span/text()")
       
        item_loader.add_xpath("bathroom_count", "//li[contains(@class,'list-group-item')]/b[.='Bathrooms']/following-sibling::span/text()")
        item_loader.add_xpath("external_id", "//li[contains(@class,'list-group-item')]/b[.='Property ID']/following-sibling::span/text()")

        available_date="".join(response.xpath("//div[@class='panel-body']/p/span[contains(.,'From')]/following-sibling::text()").getall())
        if available_date and "now" not in available_date.lower():
            date2 =  available_date.strip()
            date_parsed = dateparser.parse(
                date2, date_formats=["%m-%d-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)

        address = "".join(response.xpath("//div[@class='col-xs-12 pull-left propertyDetails']/div/div/h2/span//text()").getall())
        if address:
            item_loader.add_value("address", re.sub("\s{2,}", " ", address))
            item_loader.add_xpath("city", "//h2[contains(@class,'centerVertical')]/span[contains(@class,'textBold')]//text()")

        description = " ".join(response.xpath("//div[contains(@class,'propertyDescription')]/p/text()").getall()) 
        if description:
            item_loader.add_value("description", re.sub("\s{2,}", " ", description))

        images = [x for x in response.xpath("//div[@id='revolution-propertySlider']/ul/li//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        parking = response.xpath("//div[@class='BBCContent']/h2[small[.='Car']]/span/text()").get()
        if parking:
            if parking.strip() == "0":
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)


        dishwasher = "".join(response.xpath("//li[contains(@class,'list-group-item')]/b[.='Dishwasher']//text()").getall())
        if dishwasher:
            item_loader.add_value("dishwasher", True)

        balcony = "".join(response.xpath("//li[contains(@class,'list-group-item')]/b[.='Balcony']//text()").getall())
        if balcony:
            item_loader.add_value("balcony", True)

        latitude_longitude = response.xpath("//style[contains(.,'marker')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split("%7C-")[1].split(",")[0]
            longitude = latitude_longitude.split("%7C-")[1].split(",")[1].split("&")[0]
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)

        name = "".join(response.xpath("//div[contains(@class,'propertyAgentDetail')]/div/h2[@class='textLight staffName']//text()").getall())
        if name:
            item_loader.add_value("landlord_name", name.strip())
        else:
            item_loader.add_value("landlord_name", "Showcase Realty")
            
        item_loader.add_xpath("landlord_phone", "normalize-space(//ul[@class='agentContacts']/li/h5/a[@class='staffName']/text())")
        
        landlord_email = response.xpath("//input[contains(@name,'agency[email]')]//@value").get()
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email)

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "terrace" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "town" in p_type_string.lower() or "home" in p_type_string.lower() or "cottage" in p_type_string.lower()):
        return "house"
    else:
        return None