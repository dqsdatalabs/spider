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
from datetime import datetime
from python_spiders.helper import ItemClear
import re

class MySpider(Spider):
    name = 'prdballarat_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    start_urls = ["https://www.prdballarat.com.au/properties-for-lease"]

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[@class='block']"):
            status = item.xpath(".//img[@class='slash']/@alt").get()
            if status and "leased" in status.lower():
                continue
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        f_text = "".join(response.xpath("//h2[not(@class)]//text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            f_text = "".join(response.xpath("//div[@class='contentRegion']//text()").getall())
            if get_p_type_string(f_text):
                item_loader.add_value("property_type", get_p_type_string(f_text))
            else:
                return

        item_loader.add_value("external_id", response.url.split("/")[-2])
        item_loader.add_value("external_source", "Prdballarat_Com_PySpider_australia")   
        title = response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title", title.strip())   
        
        item_loader.add_xpath("room_count", "//div[@class='icons hide-med']/i[@class='icon-bed']/preceding-sibling::text()[1]")
        item_loader.add_xpath("bathroom_count", "//div[@class='icons hide-med']/i[@class='icon-bath']/preceding-sibling::text()[1]")
        item_loader.add_xpath("deposit", "//div[b[.='Bond:']]/text()")
     
        city =response.xpath("//h1/span[@itemprop='addressLocality']/text()").get()
        if city:
            item_loader.add_value("city", city.strip())
        address = response.xpath("//h1/span[@itemprop='streetAddress']/text()").get()
        if address:
            if city:
                address = address.strip()+", "+city
            item_loader.add_value("address", address.strip())
        item_loader.add_xpath("zipcode", "//meta[@itemprop='postalCode']/@content")

        parking = response.xpath("//div[@class='icons hide-med']/i[@class='icon-car']/preceding-sibling::text()[1]").get()
        if parking:
            if "0" in parking:
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)
        dishwasher = response.xpath("//li//text()[contains(.,'Dishwasher')]").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
    
        rent = response.xpath("//div[b[.='Rent: ']]/text()").get()
        if rent:
            rent = rent.split("$")[-1].strip().split("p")[0].replace(",","")
            item_loader.add_value("rent", int(float(rent))*4)
        item_loader.add_value("currency", "AUD")
        desc = "".join(response.xpath("//div[@class='column tiny-12 med-7']/div[@class='contentRegion']/p//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())

        available_date = response.xpath("//div[b[.='Availability:']]/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.split("Available")[-1], date_formats=["%d/%m/%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        images = [x for x in response.xpath("//meta[@property='og:image']/@content").getall()]
        if images:
            item_loader.add_value("images", images)
  
        item_loader.add_xpath("latitude", "//meta[@property='place:location:latitude']/@content")
        item_loader.add_xpath("longitude", "//meta[@property='place:location:longitude']/@content")
        item_loader.add_xpath("landlord_name", "//ul[@class='agentList']/li[1]/h4/a/text()")
        item_loader.add_xpath("landlord_email", "//ul[@class='agentList']/li[1]//a[contains(@href,'mail')]/text()")
        item_loader.add_xpath("landlord_phone", "//ul[@class='agentList']/li[1]//a[contains(@href,'tel')]/text()")
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "bovenwoning" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "home" in p_type_string.lower() or "detached" in p_type_string.lower()):
        return "house"
    else:
        return None