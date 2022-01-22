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
    name = 'lagencebb_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Lagencebb_com_PySpider_france_fr"
    
    def start_requests(self):
        start_urls = [
            {"url": "https://www.lagencebb.com/en/rental/apartment", "property_type": "apartment"},
            {"url": "https://www.lagencebb.com/en/rental/villa", "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//ul[@class='ads']/li/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            
# 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_source",  self.external_source)
        title = response.xpath("//h1//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        dontallow=response.xpath("//li[contains(.,'Availability')]/span/text()").get()
        if dontallow and "Rented"==dontallow:
            return 

        rent =  "".join(response.xpath("//h2[@class='price']/text()").extract())
        if rent:
            rent = rent.split("€")[0].replace(",","").strip()
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")
        
        deposit = response.xpath("//li[contains(.,'Guarantee')]//span//text()").get()
        if deposit:
            deposit = deposit.replace("€","").strip()
            item_loader.add_value("deposit", deposit)
        
        utilities = response.xpath("//li[contains(.,'Condominium fees')]//span//text()").get()
        if utilities:
            utilities = utilities.split("€")[0].strip()
            item_loader.add_value("utilities", utilities)

        external_id =  "".join(response.xpath("//span[@class='reference']/text()").extract())
        if external_id:
            item_loader.add_value("external_id", external_id.split(".")[1])

        meters =  "".join(response.xpath("//div[@class='summary']/ul/li[contains(. ,'Surface ')]//text()[contains(.,'m²')]").extract())
        if meters:
            s_meters = meters.split("m²")[0]
            item_loader.add_value("square_meters", s_meters)

        room_count =  ".".join(response.xpath("//li[contains(. ,'Rooms')]/span/text()").extract())
        if room_count:
            item_loader.add_value("room_count", room_count.split("r")[0])

        address =  "".join(response.xpath("//h1/text()").extract())
        if address:
            item_loader.add_value("address", address.split("- ")[-1])
            item_loader.add_value("city", address.split("- ")[-1])

        floor = "".join(response.xpath("//div[@class='summary']/ul/li[contains(. ,'Floor')]//text()[contains(.,'/')]").extract())
        if floor:
            item_loader.add_value("floor", floor.split("/")[0].replace("th",""))

        images = [x for x in response.xpath("//div[@class='show-carousel owl-carousel owl-theme']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        available_date = response.xpath("//div[@class='summary']/ul/li[contains(. ,'Available at')]//text()[contains(.,'/')]").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)

        dishwasher = "".join(response.xpath("//div[@class='services']/ul/li[contains(. ,'Dishwasher')]/text()").extract())
        if dishwasher:
            item_loader.add_value("dishwasher", True)

        washing_machine = "".join(response.xpath("//div[@class='services']/ul/li[contains(. ,'Washing machine')]/text()").extract())
        if washing_machine:
            item_loader.add_value("washing_machine", True)

        furnished = "".join(response.xpath("//div[@class='services']/ul/li[contains(. ,'Furnished')]/text()").extract())
        if furnished:
            item_loader.add_value("furnished", True)

        item_loader.add_value("landlord_phone", "+33 (0)4 95 46 82 49")
        item_loader.add_value("landlord_name", "L'AGENCE BASTIA BALAGNE")
        item_loader.add_value("landlord_email", "contact@lagencebb.com")

        yield item_loader.load_item()