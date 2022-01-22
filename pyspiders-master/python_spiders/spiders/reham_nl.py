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
from datetime import datetime
import dateparser
from ..helper import extract_number_only

class MySpider(Spider):
    name = 'reham_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.reham.nl/huizenaanbod?zoekterm=&aanbod%5B%5D=huurwoningen&min-prijs=&max-prijs=&type%5B%5D=Appartement&bouwjaar=&slaapkamers=&woonoppervlak=&perceeloppervlak=&garage=&openhuis=&sortering=plaats+ASC%2C+straat+ASC%2C+huisnummer+ASC",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.reham.nl/huizenaanbod?zoekterm=&aanbod%5B%5D=huurwoningen&min-prijs=&max-prijs=&type%5B%5D=Geschakelde+woning&type%5B%5D=Halfvrijstaande+woning&type%5B%5D=Hoekwoning&type%5B%5D=Tussenwoning&type%5B%5D=TWEE_ONDER_EEN_KAPWONING&type%5B%5D=Vrijstaande+woning&bouwjaar=&slaapkamers=&woonoppervlak=&perceeloppervlak=&garage=&openhuis=&sortering=plaats+ASC%2C+straat+ASC%2C+huisnummer+ASC",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//li[@class='object']"):
            status = " ".join(item.xpath(".//div[contains(@class,'labels')]//text()").getall())
            if status and "verhuurd" in status.lower():
                continue
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_page = response.xpath("//li[@class='next']/a/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type":response.meta["property_type"]}
            )    
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Reham_PySpider_netherlands")
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        external_id = response.url.split('-')[-1]
        if external_id:
            item_loader.add_value("external_id", external_id.strip())

        zipcode = response.xpath("//li[contains(text(),'Postcode')]/following-sibling::li[1]/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())

        address = response.xpath("//li[contains(text(),'Adres')]/following-sibling::li[1]/text()").get()
        if address:
            if zipcode: item_loader.add_value("address", address.strip() + " " + zipcode.strip())
            else: item_loader.add_value("address", address.strip())
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip())

            if "in" in title:
                city = title.split(" in ")[1].strip().split(" ")[0].strip()
                if city:
                    item_loader.add_value("city", city)
        
        square_meters = response.xpath("//li[contains(text(),'Oppervlakte woonruimte')]/following-sibling::li[1]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split('m')[0].strip())

        room_count = response.xpath("//li[contains(text(),'Aantal slaapkamers')]/following-sibling::li[1]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())

        price = 0
        rent = response.xpath("//h1/span/text()").get()
        if rent:
            rent = rent.split('€')[-1].split(',')[0].strip().replace('.', '').replace('\xa0', '')
            price = int(float(rent))
            item_loader.add_value("rent", str(int(float(rent))))
            item_loader.add_value("currency", 'EUR')
        
        deposit = response.xpath("//text()[contains(.,'Borgsom:') or contains(.,'borgsom:')]").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split('€')[-1].split(',')[0].split('.')[0].strip())
        else:
            deposit = response.xpath("//text()[contains(.,'borgsom')]").get()
            if deposit:
                deposit = deposit.split("€")[1].split(",")[0]
                item_loader.add_value("deposit", deposit)
            else:
                deposit = response.xpath("//text()[contains(.,'Borg') or contains(.,'borg')]").get()
                if deposit:
                    if ":" in deposit:
                        deposit_number = int(extract_number_only(deposit.split(":")[1]))
                        if deposit_number > 0 and deposit_number < 10:
                            item_loader.add_value("deposit", deposit_number * price)
                        else:
                            item_loader.add_value("deposit", deposit_number)
        
        from datetime import datetime
        import dateparser
        available_date = response.xpath("//text()[contains(.,'Aanvaarding')]").get()
        if available_date:
            available_date = available_date.lower().strip().split("per")[-1].replace("begin","").strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        description = " ".join(response.xpath("//div[contains(@class,'desc')]//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))

            if "vergoeding van" in description:
                utilities = description.lower().split("vergoeding van")[1].strip().split(",")[0].strip()
                item_loader.add_value("utilities", utilities)
            elif "voorschot van" in description:
                utilities = description.lower().split("voorschot van")[1].strip().split(",")[0].strip()
                item_loader.add_value("utilities", utilities)
            
            if "Borg:" in description and not item_loader.get_collected_values("deposit"):
                deposit = description.split("Borg:")[1].strip().split(" ")[0].strip()
                try:
                    deposit = int(deposit)
                    if deposit < 10:
                        deposit = deposit * price
                    item_loader.add_value("deposit", deposit)
                except:
                    pass
        
        images = [response.urljoin(x) for x in response.xpath("//div[@id='images']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

        latitude = response.xpath("//script[contains(.,'initMap')]/text()").get()
        if latitude:
            item_loader.add_value("latitude", latitude.split('lat:')[1].split(',')[0].strip())
            item_loader.add_value("longitude", latitude.split('lng:')[1].split('}')[0].strip())

        utilities = response.xpath("//text()[contains(.,'Servicekosten')]").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split('€')[-1].split(',')[0].split('.')[0].strip())

        parking = response.xpath("//li[contains(.,'Garage aanwezig')]/following-sibling::li[1]/text()").get()
        if parking:
            if parking.strip().lower() == 'ja':
                item_loader.add_value("parking", True)
            elif parking.strip().lower() == 'nee':
                item_loader.add_value("parking", False)

        furnished = response.xpath("//text()[contains(.,'Gemeubileerd') and contains(.,'-')]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        item_loader.add_value("landlord_name", "Reham Makelaars")
        item_loader.add_value("landlord_phone", "+31-115-432514")
        item_loader.add_value("landlord_email", "info@reham.nl")
        
        yield item_loader.load_item()