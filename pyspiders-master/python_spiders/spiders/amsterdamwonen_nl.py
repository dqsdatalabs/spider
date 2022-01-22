# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import scrapy


class MySpider(Spider):
    name = 'amsterdamwonen_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    custom_settings = {
        "HTTPCACHE_ENABLED": False,
        "HTTPERROR_ALLOWED_CODES": [403]
    } 
    external_source="Amsterdamwonen_PySpider_netherlands"
    # 1. FOLLOWING
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.amsterdamwonen.nl/aanbod?house_type=Appartement",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.amsterdamwonen.nl/aanbod?house_type=Woonhuis",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            yield scrapy.Request(url=url,
                                    callback=self.parse,
                                    meta={'request_url': url})
        
    
    

    def parse(self, response):
        for item in response.xpath("//div[@class='listing-houses  row m-0']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={
                    "property_type":response.meta["property_type"]})
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        status = response.xpath("//div[.='Status']/following-sibling::div/text()").get()
        if status and ("onder" in status.lower() or "verhuurd" in status.lower()):
            return

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))

        item_loader.add_value("external_source", self.external_source)

        external_id = response.url.split('-')[-1]
        if external_id:
            item_loader.add_value("external_id", external_id.strip())

        address = response.xpath("//h1/span[@class='adres']/text()").get()
        city = response.xpath("//h1/span[@class='plaatsnaam']/text()").get()
        if city:      
            item_loader.add_value("city", city.strip())
            if address: address += " " + city
        
        if address:
            item_loader.add_value("address", address.strip())
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        description = " ".join(response.xpath("//div[@id='object-description']/div[contains(@class,'small')]//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))

        square_meters = response.xpath("//div[contains(text(),'Gebruiksoppervlak wonen')]/following-sibling::div/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split('m')[0].split(',')[0].split('.')[0].strip())

        room_count = response.xpath("//div[contains(text(),'Aantal slaapkamers')]/following-sibling::div/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.xpath("//div[contains(text(),'Aantal badkamers')]/following-sibling::div/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        rent = response.xpath("//h2/text()").get()
        if rent:
            rent = rent.split('€')[-1].lower().split('p')[0].strip().replace('.', '').replace('\xa0', '')
            item_loader.add_value("rent", str(int(float(rent))))
            item_loader.add_value("currency", 'EUR')

        from datetime import datetime
        from datetime import date
        import dateparser
        available_date = response.xpath("//div[contains(text(),'Status')]/following-sibling::div/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.lower().replace('beschikbaar', 'nu').strip(), date_formats=["%d/%m/%Y"], languages=['nl'])
            today = datetime.combine(date.today(), datetime.min.time())
            if date_parsed:
                result = today > date_parsed
                if result == True:
                    date_parsed = date_parsed.replace(year = today.year + 1)
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        deposit = response.xpath("//div[contains(text(),'Waarborgsom')]/following-sibling::div/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split('€')[-1].strip().replace('.', ''))
        
        images = [response.urljoin(x) for x in response.xpath("//section[@id='object-all-photos']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        
        floor_plan_images = [response.urljoin(x) for x in response.xpath("//section[@id='object-all-photos']//img[contains(@src,'Plattegrond')]/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        try:
            latitude = response.xpath("//input[contains(@id,'mgmMarker')]/@value").get()
            if latitude:
                item_loader.add_value("latitude", latitude.split('~')[2].split(',')[0].strip())
                item_loader.add_value("longitude", latitude.split('~')[2].split(',')[1].strip())
        except:
            pass
        
        energy_label = response.xpath("//div[contains(text(),'Energielabel')]/following-sibling::div/span/text()").get()
        if energy_label:
            if energy_label.lower().split('klasse')[-1].strip().upper() in ['A', 'B', 'C', 'D', 'E', 'F', 'G']:
                item_loader.add_value("energy_label", energy_label.lower().split('klasse')[-1].strip().upper())
        
        parking = response.xpath("//div[contains(text(),'Soort parkeergelegenheid')]/following-sibling::div/text()").get()
        if parking:
            if 'parkeren' in parking.strip().lower():
                item_loader.add_value("parking", True)

        item_loader.add_value("landlord_name", "Amsterdam Wonen Real Estate")
        item_loader.add_xpath("landlord_phone", "//li[@class='detail-contact-phone']//a/text()")
        item_loader.add_xpath("landlord_email", "//li[@class='detail-contact-email']//a/text()")
        
        yield item_loader.load_item()
