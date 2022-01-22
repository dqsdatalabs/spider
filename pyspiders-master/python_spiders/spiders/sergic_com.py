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
    name = 'sergic_com'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):
        start_urls = [
            {
                "url": "https://www.sergic.com/wp-json/sergic/v1/post?params%5Bcontract_type%5D=location&params%5Bplace_types%5D%5B%5D=appartement&params%5Bdispo%5D=all&params%5Blocalisation_srch%5D=false&params%5Bexpanse_srch%5D=0&params%5Bappt_min_area%5D=0&params%5Bprice_min%5D=0&params%5Bprice_max%5D=5000&params%5Bref%5D=&params%5BisRef%5D=false&params%5Bzoomed%5D=&params%5BcitySearch%5D=&params%5Blat_move_map%5D=&params%5Blng_move_map%5D=&params%5Bzoom_move_map%5D=", 
                "property_type": "apartment"
            },
	        {
                "url": "https://www.sergic.com/wp-json/sergic/v1/post?params%5Bcontract_type%5D=location&params%5Bplace_types%5D%5B%5D=maison&params%5Bdispo%5D=all&params%5Blocalisation_srch%5D=false&params%5Bexpanse_srch%5D=0&params%5Bappt_min_area%5D=0&params%5Bprice_min%5D=0&params%5Bprice_max%5D=5000&params%5Bref%5D=&params%5BisRef%5D=false&params%5Bzoomed%5D=&params%5BcitySearch%5D=&params%5Blat_move_map%5D=&params%5Blng_move_map%5D=&params%5Bzoom_move_map%5D=", 
                "property_type": "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)
        for item in data:
            url = item['link']
            yield Request(url, callback=self.populate_item, meta={'property_type':response.meta.get('property_type'), 'item':item})

    # # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item = response.meta.get('item')
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))

        item_loader.add_value("external_source", "Sergic_PySpider_france")

        external_id = item['ref']
        if external_id:
            item_loader.add_value("external_id", external_id)

        title = response.xpath("//h3//text()").get()
        if title:
            item_loader.add_value("title", title.strip())

        address = item['localisation']
        if address:
            item_loader.add_value("address", address.strip())

        city = item['city']
        if city:
            item_loader.add_value("city", city.strip())

        zipcode = item['postal_code']
        if zipcode:
            item_loader.add_value("zipcode", zipcode)

        square_meters = item['area']
        if square_meters:
            item_loader.add_value("square_meters", square_meters.strip())

        rent = item['price']
        if rent:
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        deposit = response.xpath("//p[contains(@class,'appt-desc__budget-price-info')]//text()[contains(.,'Dépot de garantie')]").get()
        if deposit:
            deposit = deposit.split(":")[1].replace("€","").strip()
            item_loader.add_value("deposit", deposit)

        utilities = response.xpath("//p[contains(@class,'appt-desc__budget-price-info')]//text()[contains(.,'Provision pour charges')]").get()
        if utilities:
            utilities = utilities.split(":")[1].split("€")[0].split(",")[0].strip()
            item_loader.add_value("utilities", utilities)

        desc = " ".join(response.xpath("//div[contains(@class,'appt-desc__description-text-paragraph')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//span[@class='appt-desc__caracteristique-text'][contains(text(),'pièces')]/text()").get()
        if room_count:
            room_count = room_count.split()[0]
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//span[contains(@class,'appt-desc__caracteristique-text')]//text()[contains(.,'salle')]").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip().split(" ")[0]
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[contains(@class,'carousel-list')]//@data-lazy-src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//span[contains(.,'Disponible le')]//text()").getall())
        if available_date:
            available_date = available_date.split(":")[1].strip()
            if not "now" in available_date.lower():
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        energy_label = response.xpath("//div[contains(@id,'selected-panel-letter')]//text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)

        latitude = item['lat']
        if latitude:
            item_loader.add_value("latitude", latitude)

        longitude = item['lng']
        if longitude:
            item_loader.add_value("longitude", longitude)

        item_loader.add_value("landlord_name", "Sergic")
        
        yield item_loader.load_item()