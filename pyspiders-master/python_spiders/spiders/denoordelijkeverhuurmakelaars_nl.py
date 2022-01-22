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
    name = 'denoordelijkeverhuurmakelaars_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.denoordelijkeverhuurmakelaars.nl/aanbod?searchon=list&sorts=Flat",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.denoordelijkeverhuurmakelaars.nl/aanbod?searchon=list&sorts=Dwelling",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://www.denoordelijkeverhuurmakelaars.nl/aanbod?searchon=list&sorts=Room",
                ],
                "property_type" : "room"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@data-view='showOnList']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_page = response.xpath("//div[contains(@class,'paging-next')]/a/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type":response.meta["property_type"]}
            )    
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Denoordelijkeverhuurmakelaars_PySpider_netherlands")

        external_id = response.xpath("//td[contains(.,'Referentie')]/following-sibling::td/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())

        address = response.xpath("normalize-space(//h1/text()[1])").get()
        if address:
            item_loader.add_value("address", address.strip())


        a_city = response.xpath("substring-after(normalize-space(//h1/text()[1]),', ')").get()
        if a_city:
            city = a_city.split(" ")[-1].strip()
            zipcode =a_city.split(city)[0]
            item_loader.add_value("zipcode", zipcode)   
            item_loader.add_value("city",city )   
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        description = " ".join(response.xpath("//h2[contains(.,'Omschrijving')]/following-sibling::p//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))

        square_meters = response.xpath("//td[contains(.,'Totale woonoppervlakte')]/following-sibling::td/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split('m')[0].strip())

        room_count = response.xpath("//td[contains(.,'Slaapkamers')]/following-sibling::td/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())

        item_loader.add_xpath("latitude","substring-before(substring-after(//script[contains(.,'latitude')]/text(),'latitude: '),',')") 
        item_loader.add_xpath("longitude","substring-before(substring-after(//script[contains(.,'latitude')]/text(),'longitude: '),',')")
        
        if not item_loader.get_collected_values("room_count"):
            if response.xpath("//td[contains(.,'Kamers')]/following-sibling::td/text()").get():
                item_loader.add_value("room_count", response.xpath("//td[contains(.,'Kamers')]/following-sibling::td/text()").get().strip())
        
        bathroom_count = response.xpath("//td[contains(.,'Badkamers')]/following-sibling::td/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        rent = response.xpath("//h1/text()[contains(.,'Prijs')]").get()
        if rent:
            rent = rent.split('€')[-1].split('/')[0].strip().replace('.', '').replace('\xa0', '')
            item_loader.add_value("rent", str(int(float(rent))))
            item_loader.add_value("currency", 'EUR')

        from datetime import datetime
        from datetime import date
        import dateparser
        available_date = response.xpath("//td[contains(.,'Beschikbaar vanaf')]/following-sibling::td/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.strip().lower().replace('per direct', 'nu'), date_formats=["%d/%m/%Y"], languages=['nl'])
            today = datetime.combine(date.today(), datetime.min.time())
            if date_parsed:
                result = today > date_parsed
                if result == True:
                    date_parsed = date_parsed.replace(year = today.year + 1)
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        images = [response.urljoin(x) for x in response.xpath("//div[@id='owl-pic-pictures']/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        
        floor = response.xpath("//td[contains(.,'Op verdieping')]/following-sibling::td/text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())

        utilities = response.xpath("//text()[contains(.,'servicekosten') and contains(.,'bedragen')]").get()
        if utilities:
            item_loader.add_value("utilities", "".join(filter(str.isnumeric, utilities.strip())))

        furnished = response.xpath("//td[contains(.,'Gestoffeerd')]/following-sibling::td/text()").get()
        if furnished:
            if furnished.strip().lower() == 'geheel':
                item_loader.add_value("furnished", True)

        item_loader.add_value("landlord_name", "De Noordelijke Verhuur Makelaars")
        item_loader.add_value("landlord_phone", "+31 (0) 50 31 14 013")
        item_loader.add_value("landlord_email", "info@denoordelijkeverhuurmakelaars.nl ")
              
        yield item_loader.load_item()