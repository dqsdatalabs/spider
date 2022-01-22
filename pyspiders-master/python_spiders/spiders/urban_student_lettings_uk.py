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

class MySpider(Spider): 
    name = 'urban_student_lettings_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.urban-student-lettings.uk/lettings/properties?branch=&type=flat&min-rent=&max-rent=&min-bedrooms=&max-bedrooms=",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.urban-student-lettings.uk/lettings/properties?branch=&type=house&min-rent=&max-rent=&min-bedrooms=&max-bedrooms="
                ],
                "property_type": "house"
            },
            {
                "url": [
                    "https://www.urban-student-lettings.uk/lettings/properties?branch=&type=studio&min-rent=&max-rent=&min-bedrooms=&max-bedrooms=",
                ],
                "property_type": "studio"
            },
            {
                "url": [
                    "https://www.urban-student-lettings.uk/lettings/properties?branch=&type=room&min-rent=&max-rent=&min-bedrooms=&max-bedrooms=",
                ],
                "property_type": "room"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@class='images']"):
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            status = item.xpath("./span[@class='status']/text()").get()
            if not status:
                yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
        
        next_page = response.xpath("//li[contains(.,'Next')]//@href").get()
        if next_page:
            yield Request(next_page, callback=self.parse, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("-")[-1])
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Urban_Student_Lettings_PySpider_united_kingdom")

        title = "".join(response.xpath("//h1//text()").getall())
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))

        address = response.xpath("//script[contains(.,'latitude')]/text()").get()
        if address:
            data = json.loads(address)
            item_loader.add_value("address", f"{data['address'][0]['addressLocality']} {data['address'][0]['addressRegion']} {data['address'][0]['postalCode']} {data['address'][0]['streetAddress']}")
            item_loader.add_value("city", data["address"][0]["addressRegion"])
            item_loader.add_value("zipcode", data["address"][0]["postalCode"])
            # item_loader.add_value("latitude", data["geo"][0]["latitude"])
            # item_loader.add_value("longitude", data["geo"][0]["longitude"])
        
        rent = response.xpath("normalize-space(//span[@class='price']/text())").get()
        if rent:
            rent = rent.split(" ")[0].replace("£","").replace(",","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")
        
        room_count = response.xpath("//li[@class='bedroom-count']/text() | //li[@class='reception-room-count']/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(":")[1].strip())
        
        bathroom_count = response.xpath("substring-after(//li[@class='bathroom-count']/text(),':')").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        import dateparser
        available_date = response.xpath("//li[contains(.,'Available')]/time/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        description = " ".join(response.xpath("//div[@id='property-details']//p//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        
        furnished = response.xpath("//li[contains(.,'Furnished')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        energy_label = response.xpath("//div[@id='property-epc']/img/@src").get()
        if energy_label and "currentenergy=" in energy_label:
            energy_label = energy_label.split("currentenergy=")[1].split("&")[0]
            item_loader.add_value("energy_label", energy_label)
        
        images = [x.split("(")[1].split(")")[0] for x in response.xpath("//div[@class='carousel-inner']//@style").getall()]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_value("landlord_name", "Urban Home")
        item_loader.add_value("landlord_phone", "02392 836591")
        item_loader.add_value("landlord_email", " emergencies@urban-home.co.uk")
        
        yield item_loader.load_item()