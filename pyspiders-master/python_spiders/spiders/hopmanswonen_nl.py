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
    name = 'hopmanswonen_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.hopmanswonen.nl/aanbod/particulier/?filters=woningtype[appartement]",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.hopmanswonen.nl/aanbod/particulier/?filters=woningtype[studio]",
                ],
                "property_type" : "studio"
            },
            {
                "url" : [
                    "https://www.hopmanswonen.nl/aanbod/particulier/?filters=woningtype[kamer]",
                ],
                "property_type" : "room"
            },
            {
                "url" : [
                    "https://www.hopmanswonen.nl/aanbod/particulier/?filters=woningtype[woonhuis]",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[contains(@class,'woocommerce-loop-product__link')]"):
            status = item.xpath("./div[@class='image-wrapper']/div[@class='status']/text()").get()
            if status and "verhuurd" in status.lower():
                continue
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type']})
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))

        item_loader.add_value("external_source", "Hopmanswonen_PySpider_netherlands")

        address1 = response.xpath("//h2[@class='plaats']/text()").get()
        address2 = response.xpath("//div[@class='adres']/text()").get()
        if address1 and address2:
            item_loader.add_value("address", address1.strip() + " " + address2.strip())
            item_loader.add_value("city", address1)
        
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        description = " ".join(response.xpath("//h2[contains(.,'Over dit object')]/following-sibling::*//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))

        square_meters = response.xpath("//div[contains(text(),'Woonoppervlakte')]/following-sibling::div/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split('m')[0].strip())

        room_count = response.xpath("//div[contains(text(),'Aantal slaapkamers')]/following-sibling::div/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        elif response.meta.get('property_type') == "studio":
            item_loader.add_value("room_count", "1")

        rent = "".join(response.xpath("//div[@class='prices tablet-desktop']//text()").getall())
        if rent:
            rent = rent.split('€')[-1].split(',')[0].strip().replace('.', '').replace('\xa0', '')
            item_loader.add_value("rent", str(int(float(rent))))
            item_loader.add_value("currency", 'EUR')

        from datetime import datetime
        from datetime import date
        import dateparser
        available_date = response.xpath("//div[contains(text(),'Beschikbaar vanaf')]/following-sibling::div/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d/%m/%Y"], languages=['nl'])
            today = datetime.combine(date.today(), datetime.min.time())
            if date_parsed:
                result = today > date_parsed
                if result == True:
                    date_parsed = date_parsed.replace(year = today.year + 1)
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        deposit = "".join(response.xpath("//span[contains(text(),'Waarborgsom')]/following-sibling::span//text()").getall())
        if deposit:
            item_loader.add_value("deposit", deposit.split('€')[-1].split(',')[0].strip().replace('.', ''))
        
        images = [response.urljoin(x) for x in response.xpath("//div[@class='gallery-wrapper']//figure//a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

        latitude = response.xpath("//div[@class='maps-streeview-wrapper']/iframe/@streetview-src").get()
        if latitude:
            item_loader.add_value("latitude", latitude.split('location=')[1].split(',')[0].strip())
            item_loader.add_value("longitude", latitude.split('location=')[1].split(',')[1].strip())
        
        energy_label = response.xpath("//div[contains(text(),'Energielabel')]/following-sibling::div/text()").get()
        if energy_label:
            if energy_label.strip().upper() in ['A', 'B', 'C', 'D', 'E', 'F', 'G']:
                item_loader.add_value("energy_label", energy_label.strip().upper())
            else:
                label = response.xpath("//div[div[.='Energielabel']]/div[2]/text()").get()
                if label!= "niet beschikbaar":
                    item_loader.add_value("energy_label", label) 

        utilities = "".join(response.xpath("//div[contains(text(),'Servicekosten VVE')]/following-sibling::div//text()").getall())
        if utilities:
            item_loader.add_value("utilities", utilities.split('€')[-1].split(',')[0].strip().replace('.', ''))
        
        parking = response.xpath("//div[contains(text(),'parkeren') or contains(text(),'Parkeren')]").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//div[contains(text(),'balkon') or contains(text(),'Balkon')]").get()
        if balcony:
            item_loader.add_value("balcony", True)

        furnished = response.xpath("//img[contains(@src,'oplevering')]/following-sibling::div/text()").get()
        if furnished:
            if furnished.strip().lower() == 'gemeubileerd':
                item_loader.add_value("furnished", True)
            elif furnished.strip().lower() == 'ongemeubileerd' or furnished.strip().lower() == 'kaal':
                item_loader.add_value("furnished", False)

        terrace = response.xpath("//div[contains(text(),'dakterras') or contains(text(),'Dakterras')]").get()
        if terrace:
            item_loader.add_value("terrace", True)

        item_loader.add_value("landlord_name", "Hopmans Wonen")
        item_loader.add_value("landlord_phone", "+31 164 85 71 51")
        item_loader.add_value("landlord_email", "info@hopmanswonen.nl")
      
        yield item_loader.load_item()