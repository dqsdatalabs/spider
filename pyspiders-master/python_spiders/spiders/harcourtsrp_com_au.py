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

class MySpider(Spider):
    name = 'harcourtsrp_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    start_urls = ["https://harcourtsrp.com.au/results"]
    custom_settings = {
        "PROXY_TR_ON" : True
    }

    headers = {           
        'accept': '*/*',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'tr,en;q=0.9',
        'content-type': 'application/json;charset=utf-8',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.111 YaBrowser/21.2.1.107 Yowser/2.5 Safari/537.36'

    }


    def parse(self,response):

        token = response.xpath("//meta[@name='csrf-token']/@content").extract_first()
        prop_type = ["Apartment","House"]

        for p in prop_type:

            formdata = {
                "authenticityToken": f"{token}",
                "_method": "post",
                "LISTING_SALE_METHOD": "Lease",
                "LISTING_CATEGORY": "Residential",
                "listing_property_type": f"{p}",
                "LISTING_BEDROOMS": "",
                "LISTING_PRICE_FROM": "",
            }

            yield FormRequest(
                url="https://harcourtsrp.com.au/results",
                callback=self.parse_listing,
                formdata=formdata,
                meta={"property_type":p}
            )
 
    # 1. FOLLOWING
    def parse_listing(self, response):

        items = response.xpath("//div[@class='content-container']/ul/li/a/@href").getall()
        if items:
            for item in items:
                yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Harcourtsrp_Com_PySpider_australia")
        item_loader.add_xpath("title", "//title/text()")
        
        item_loader.add_value("address", item_loader.get_collected_values("title")[0].split('::')[1].strip())
        item_loader.add_value("zipcode", item_loader.get_collected_values("address")[0].split(',')[-1].strip())
        item_loader.add_value("city", item_loader.get_collected_values("address")[0].split(',')[-2].strip())

        rent = "".join(response.xpath("normalize-space(//div[@class='property-information']/h3[contains(.,'Lease')]/following-sibling::text())").extract())
        if rent:
            price = rent.split(" ")[0].replace("\xa0",".").replace(",","").replace(" ","").replace("$","").strip()
            if price.replace(".","").isdigit():
                item_loader.add_value("rent", int(float(price))*4)
            item_loader.add_value("currency", "AUD")

        deposit = "".join(response.xpath("normalize-space(//div[@class='property-information']/h3[contains(.,'Bond')]/following-sibling::text())").extract())
        if deposit:
            deposit = deposit.split(" ")[0].replace("\xa0",".").replace(",","").replace(" ","").replace("$","").strip()
            item_loader.add_value("deposit", deposit)

        available_date = "".join(response.xpath("normalize-space(//div[@class='property-information']/h3[contains(.,'Available')]/following-sibling::text())").getall())
        if available_date:
            if not "now" in available_date.lower():
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        desc = " ".join(response.xpath("//div[@class='description']/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
        item_loader.add_xpath("latitude", "substring-before(substring-after(//script/text()[contains(.,'latLng')],'lat: '),',')")
        item_loader.add_xpath("longitude", "substring-before(substring-after(//script/text()[contains(.,'latLng')],'lng: '),'}')")

        room = " ".join(response.xpath("substring-before(//div[@class='description']/div[@class='bbc']/text()[contains(.,'Bed')],'Bed')").extract())
        if room:
            item_loader.add_value("room_count", room.strip())

        bathroom_count = " ".join(response.xpath("substring-after(substring-before(//div[@class='description']/div[@class='bbc']/text()[contains(.,'Bath')],'Bath'),'|')").extract())
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
            
        images = [response.urljoin(x) for x in response.xpath("//div[@style='display:none;']/a/@href").extract()]
        if images:
            item_loader.add_value("images", images)

        parking ="".join(response.xpath("//div[@class='description']/div[@class='bbc']/text()[contains(.,'Car')]").extract())   
        if parking:
            park = parking.split("|")[-1].strip()
            (item_loader.add_value("parking", True) if park !="0" else item_loader.add_value("parking", False))

        dishwasher ="".join(response.xpath("//div[@class='feature-list']/text()[contains(.,'Dishwasher')]").extract())   
        if dishwasher:
            item_loader.add_value("dishwasher", True)

        item_loader.add_value("landlord_phone", "03 9333 7999")
        item_loader.add_value("landlord_name", "Harcourts Roxburgh Park")  
        item_loader.add_value("landlord_email", "con.lonigro@harcourtsrp.com.au")  
        yield item_loader.load_item()