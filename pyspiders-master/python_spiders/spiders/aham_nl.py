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
    name = 'aham_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    
    def start_requests(self):

        start_urls = [
            {
                "property_type" : "apartment",
                "type" : "Appartement"
            },
            {
                "property_type" : "apartment",
                "type" : "Benedenwoning"
            },
        ]

        for item in start_urls:
            formdata = {
                "action": "ll_filter_properties",
                "form_fields[0][name]": "type",
                "form_fields[0][value]": item.get("type"),
                "form_fields[1][name]": "sort-appartments",
                "form_fields[1][value]": "2",
                "form_fields[2][name]": "input-lang",
                "form_fields[2][value]": "nl",
            }
            yield FormRequest(
                "https://www.aham.nl/wp-admin/admin-ajax.php",
                callback=self.parse,
                formdata=formdata,
                dont_filter=True,
                meta={
                    "property_type":item["property_type"]
                }

            )
       


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[contains(@class,'col-3')]/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type']})

        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Aham_PySpider_netherlands")

        address = response.xpath("//td[contains(.,'Adres')]/following-sibling::td/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.split(',')[-1].strip())
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        description = " ".join(response.xpath("//div[@id='house-description']/p//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))

        square_meters = response.xpath("//td[contains(.,'Oppervlakte')]/following-sibling::td/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split('m')[0].strip())

        room_count = response.xpath("//td[contains(.,'Aantal slaapkamers')]/following-sibling::td/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())

        rent = response.xpath("//td[contains(.,'Huurprijs')]/following-sibling::td/text()").get()
        if rent:
            rent = rent.split('€')[-1].split(',')[0].strip().replace('.', '').replace('\xa0', '')
            item_loader.add_value("rent", str(int(float(rent))))
            item_loader.add_value("currency", 'EUR')

        from datetime import datetime
        from datetime import date
        import dateparser
        available_date = response.xpath("//td[contains(.,'Beschikbaar vanaf')]/following-sibling::td/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d/%m/%Y"], languages=['nl'])
            today = datetime.combine(date.today(), datetime.min.time())
            if date_parsed:
                result = today > date_parsed
                if result == True:
                    date_parsed = date_parsed.replace(year = today.year + 1)
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        deposit = response.xpath("//td[contains(.,'Waarborgsom')]/following-sibling::td/text()").get()
        if deposit:
            if 'twee' in deposit.lower() or '2' in deposit.lower():
                item_loader.add_value("deposit", str(int(rent) * 2))
        
        images = [response.urljoin(x.split('url(')[-1].split(')')[0].strip()) for x in response.xpath("//div[@class='carousel-main']/div/@style").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        
        floor_plan_images = [response.urljoin(x) for x in response.xpath("//h2[contains(.,'Plattegrond')]/../a/@href").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        latitude = response.xpath("//script[contains(.,'woningPois')]/text()").get()
        if latitude:
            item_loader.add_value("latitude", latitude.split(',')[2].strip())
            item_loader.add_value("longitude", latitude.split(',')[3].strip())

        utilities = response.xpath("//td[contains(.,'Service kosten')]/following-sibling::td/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split('€')[-1].split(',')[0].strip().replace('.', ''))

        item_loader.add_value("landlord_name", "AHAM Vastgoed")
        item_loader.add_value("landlord_phone", "020 62 461 01")
        item_loader.add_value("landlord_email", "aham@aham.nl")

        yield item_loader.load_item()

