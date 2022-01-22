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

class MySpider(Spider):
    name = 'areal_com_au'
    execution_type='testing'
    country='australia'
    locale='en'

    def start_requests(self):
        start_urls = [
            {"url": "https://www.areal.com.au/search-results/?list=lease&keywords"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@id='thumbnail-mode']/div[contains(@class,'listing')]"):
            follow_url = response.urljoin(item.xpath(".//a/@href").get())
            prop_type = item.xpath(".//p[@class='property_type']/text()").get()
            property_type = ""
            if "apartment" in prop_type.lower():
                property_type = "apartment"
            elif "house" in prop_type.lower():
                property_type = "house"
            elif "studio" in prop_type.lower():
                property_type = "studio"
            elif "unit" in prop_type.lower():
                property_type = "house"
            elif "townhouse" in prop_type.lower():
                property_type = "house"
            if property_type != "":
                yield Request(follow_url, callback=self.populate_item, meta={'property_type' : property_type})

        next_page = response.xpath("//p[@class='page_next']/a/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Areal_Com_PySpider_australia")
        item_loader.add_value("property_type",  response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        title = " ".join(response.xpath("//div[@class='property-header']/h2/span/text()").getall())
        if title:
            item_loader.add_value("title", title.strip())

        price=""
        rent = "".join(response.xpath("//div[@class='opentimes']/div/p[@class='price'][contains(.,'$')]/text()").extract())     
        if rent:      
            if "-" in rent:
                price = rent.split("-")[1].replace("/"," ").strip().split(" ")[0].split("$")[1].replace(",","").strip()
            else:
                price =  rent.replace("/"," ").split(" ")[0].split("$")[1].replace(",","").strip()
            item_loader.add_value("rent",int(float(price))*4)
        item_loader.add_value("currency","AUD")

        deposit = "".join(response.xpath("//div[@class='opentimes']/div/p[@class='price'][contains(.,'Bond')]/text()").extract())
        if deposit:
            dep =  deposit.split(" ")[1].strip()
            item_loader.add_value("deposit",dep)

        available_date="".join(response.xpath("//div[@class='opentimes']/div/p[@class='date']/text()").getall())
        if available_date:
            date2 =  available_date.split(":")[1].strip().replace("Immediate","now")
            date_parsed = dateparser.parse(
                date2, date_formats=["%m-%d-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)

        item_loader.add_xpath("room_count", "//li[@class='bedrooms']/span[contains(@class,'num')]/text()")
        item_loader.add_xpath("bathroom_count", "//li[@class='bathrooms']/span[contains(@class,'num')]/text()")

        item_loader.add_value("external_id", response.url.split("/")[-2].strip())

        address = " ".join(response.xpath("//h2[contains(@class,'address-suburb')]//text()").getall())
        if address:
            item_loader.add_value("address", re.sub("\s{2,}", " ", address))
            city = response.xpath("//h2[contains(@class,'address-suburb')]/span[@class='suburb']/text()").extract_first()
            if len(city.strip().split(' ')) > 3:
                item_loader.add_value("city", " ".join(city.split(" ")[:2]))
                item_loader.add_value("zipcode", " ".join(city.split(" ")[2:]))
            else:
                item_loader.add_value("city",city.split(" ")[0].strip() )
                item_loader.add_value("zipcode"," ".join(city.split(" ")[1:]))

        item_loader.add_xpath("latitude","substring-before(//meta[@name='geo.position']/@content,';')")
        item_loader.add_xpath("longitude","substring-after(//meta[@name='geo.position']/@content,';')")

        desc =  " ".join(response.xpath("//div[@class='copy']/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        images = [ x for x in response.xpath("//div[@class='item image']/img/@src").getall()]
        if images:
            item_loader.add_value("images", images) 

        parking = "".join(response.xpath("//li[@class='carspaces']/span[contains(@class,'num')]/text()").extract())      
        if parking:
            (item_loader.add_value("parking", True) if "0" not in parking else item_loader.add_value("parking", False))
           

        pets_allowed = "".join(response.xpath("//div[@class='copy']/text()[contains(.,'Pets')]").extract())
        if pets_allowed:
            if "yes" in pets_allowed.lower() :
                item_loader.add_value("pets_allowed", True)
            elif "no" in pets_allowed.lower():
                item_loader.add_value("pets_allowed", False)

        elevator = "".join(response.xpath("//div[@class='copy']/text()[contains(.,'Lift')]").extract())
        if elevator:
            item_loader.add_value("elevator", True)

        item_loader.add_xpath("landlord_name", "//div[@class='agent_contact_info']/h4//text()")
        item_loader.add_xpath("landlord_phone", "//div[@class='agent_contact_info']/p[@class='agent_phone']/span/a/text()")


        yield item_loader.load_item()