# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'johnsonsandpartners_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    
    def start_requests(self):

        start_urls = [
            {"url": "https://www.johnsonsandpartners.co.uk/search/?showstc=off&showsold=off&instruction_type=Letting", "property_type": "apartment"}
        ] 
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='property']//div/h2/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])

        item_loader.add_value("external_source", "Johnsonsandpartners_Co_PySpider_united_kingdom")

        external_id = response.url.split("property-details/")[1].split("/")[0].strip()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())

        address = " ".join(response.xpath("//div[@class='col-md-5']/h1/text()").getall()).strip()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.strip().split(',')[-1].strip())
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        description = " ".join(response.xpath("//div[@class='col-md-5']//div/p/text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))


        room_count = response.xpath("//div[@class='room-icons']/span[1]/strong/text()").get()
        if room_count:
            if room_count != "0":
                item_loader.add_value("room_count", room_count.strip())

        bathroom_count = response.xpath("//div[@class='room-icons']/span[2]/strong/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        rent = response.xpath("//div[@class='col-md-5']/h2/text()[contains(.,'£')]").get()
        if rent:
            rent_val = rent.split('£')[-1].strip().replace(',', '').replace('\xa0', '').replace("PCM","")
            item_loader.add_value("rent",rent_val.strip())
            item_loader.add_value("currency", 'GBP')


        from datetime import datetime
        from datetime import date
        import dateparser
        available_date = response.xpath("//li[contains(.,'Available')]/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.lower().split('available')[-1].split('from')[-1].strip(), date_formats=["%d/%m/%Y"], languages=['en'])
            today = datetime.combine(date.today(), datetime.min.time())
            if date_parsed:
                result = today > date_parsed
                if result == True:
                    date_parsed = date_parsed.replace(year = today.year + 1)
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        images = [response.urljoin(x) for x in response.xpath("//div[@class='carousel-inner']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        

        latitude = response.xpath("substring-after(//script[@type='application/ld+json']//text(),'latitude')").get()
        if latitude:
            item_loader.add_value("latitude", latitude.split(':')[1].split(",")[0].split('"')[0].strip())
            item_loader.add_value("longitude", latitude.split('"longitude": "')[1].split('"')[0].strip())

        energy_label = "".join(response.xpath("//div[@class='col-md-4']/ul/li[contains(.,'EPC')]/text()").getall())
        if energy_label:
            energy_label = energy_label.replace("EPC","")
            item_loader.add_value("energy_label", energy_label.strip())
        
        floor = response.xpath("//div[@class='col-md-8']/h3[contains(.,'Floor')]/text()").get()
        if floor:
            item_loader.add_value("floor", floor.split(" ")[0])
        
        pets_allowed = "".join(response.xpath("//div[@class='col-md-4']/ul/li[contains(.,'NO PETS')]/text()").getall())
        if pets_allowed:
            item_loader.add_value("pets_allowed", False)


        item_loader.add_value("landlord_name", "Johnsons and Partners")
        item_loader.add_value("landlord_phone", "0115 931 2020")
        item_loader.add_value("landlord_email", "enquiries@johnsonsandpartners.co.uk")

        yield item_loader.load_item()
