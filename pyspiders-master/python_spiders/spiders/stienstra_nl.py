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
    name = 'stienstra_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    external_source = 'Stienstra_PySpider_netherlands'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.stienstra.nl/uitgebreid-zoeken/page/{}/?type=Appartement&status=te-huur&min-area=0%20m%C2%B2&max-area=600%20m%C2%B2&min-price=%E2%82%AC0&max-price=%E2%82%AC700.000",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.stienstra.nl/uitgebreid-zoeken/page/{}/?type=Vrijstaand&status=te-huur&min-area=0%20m%C2%B2&max-area=600%20m%C2%B2&min-price=%E2%82%AC0&max-price=%E2%82%AC700.000",
                    "https://www.stienstra.nl/uitgebreid-zoeken/page/{}/?type=Tussenwoning&status=te-huur&min-area=0%20m%C2%B2&max-area=600%20m%C2%B2&min-price=%E2%82%AC0&max-price=%E2%82%AC700.000",
                    "https://www.stienstra.nl/uitgebreid-zoeken/page/{}/?type=Hoekwoning&status=te-huur&min-area=0%20m%C2%B2&max-area=600%20m%C2%B2&min-price=%E2%82%AC0&max-price=%E2%82%AC700.000",
                    "https://www.stienstra.nl/uitgebreid-zoeken/page/{}/?type=Bungalow&status=te-huur&min-area=0%20m%C2%B2&max-area=600%20m%C2%B2&min-price=%E2%82%AC0&max-price=%E2%82%AC700.000",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format("1"),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base_url":item})


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//a[contains(@class,'hover-effect')]/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type']})
            seen = True
        
        if page == 2 or seen:
            base_url = response.meta["base_url"]
            p_url = base_url.format(str(page))
            yield Request(
                p_url,
                callback=self.parse,
                meta={'property_type': response.meta['property_type'], "base_url":base_url, "page":page+1}
            )
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        status_check = response.xpath("//span[@class='label-status label label-default']/text()").get()
        if status_check and "te koop" in status_check.lower():
            return
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        if response.url == "https://www.stienstra.nl/":
            return
        item_loader.add_value("external_source", self.external_source)

        address = response.xpath("normalize-space(//h1/text())").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.strip().split(" ")[-1].strip())
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        description = " ".join(response.xpath("//div[@id='description']/p[.='Introductie']/following-sibling::*/text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.strip())

        square_meters = response.xpath("//span[contains(.,'Woonoppervlakte')]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split('m')[0].strip())

        room_count = response.xpath("//span[contains(.,'Slaapkamers')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
     
        rent = response.xpath("//section[contains(@class,'detail-top')]//span[@class='item-price']/text()").get()
        if rent:
            rent = rent.split('€')[-1].split(' ')[0].strip().replace('\xa0', '').replace(".000","").replace(".","")
            item_loader.add_value("rent", int(float(rent)))
            item_loader.add_value("currency", 'EUR')

        from datetime import datetime
        from datetime import date
        import dateparser
        available_date = response.xpath("//li[contains(.,'Beschikbaar')]/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.split('per')[-1].strip(), date_formats=["%d/%m/%Y"], languages=['nl'])
            today = datetime.combine(date.today(), datetime.min.time())
            if date_parsed:
                result = today > date_parsed
                if result == True:
                    date_parsed = date_parsed.replace(year = today.year + 1)
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        images = [response.urljoin(x) for x in response.xpath("//div[@id='gallery']//div[contains(@class,'slider-nav ')]//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        
        floor_plan_images = [response.urljoin(x) for x in response.xpath("//div[@id='floor_plan']//img/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        latitude = response.xpath("//script[contains(.,'lat =')]/text()").get()
        if latitude:
            item_loader.add_value("latitude", latitude.split("lat = '")[1].split("'")[0].strip())
            item_loader.add_value("longitude", latitude.split("lng = '")[1].split("'")[0].strip())
        
        energy_label = response.xpath("//li[contains(.,'Energielabel')]/text()").get()
        if energy_label:
            if energy_label.split('Energielabel')[-1].strip().upper() in ['A', 'B', 'C', 'D', 'E', 'F', 'G']:
                item_loader.add_value("energy_label", energy_label.split('Energielabel')[-1].strip().upper())

        utilities = response.xpath("//strong[contains(.,'Servicekosten')]/../following-sibling::p[1]/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split('€')[-1].strip().split(' ')[0].split(',')[0].strip())
        
        parking = response.xpath("//div[@id='features']//li[contains(.,'parkeerplaats')]//text() | //li[contains(.,'Garage') or contains(.,'garage')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//li[contains(.,'Balkon') or contains(.,'balkon')]").get()
        if balcony:
            item_loader.add_value("balcony", True)

        terrace = response.xpath("//li[contains(.,'Terras') or contains(.,'terras')]").get()
        if terrace:
            item_loader.add_value("terrace", True)

        item_loader.add_value("landlord_name", "Stienstra")
        item_loader.add_value("landlord_phone", "045-5638300")
      
        yield item_loader.load_item()