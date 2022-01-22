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
    name = 'agencebsh_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Agencebsh_PySpider_france"
    custom_settings = {
        "PROXY_TR_ON": True,
        "CONCURRENT_REQUESTS" : 4,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "RETRY_TIMES": 3,
        "DOWNLOAD_DELAY": 1,
    }
    
    def start_requests(self):
        start_urls = [
            {"url": "https://www.agencebsh.com/a-louer/appartements/1", "property_type": "apartment"},
	        {"url": "https://www.agencebsh.com/a-louer/maisons-villas/1", "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        seen = False
        for item in response.xpath("//article[contains(@class,'panelBien')]/@onclick").extract():
            f_url = item.split("'")[1]
            yield Request(
                response.urljoin(f_url),
                callback=self.populate_item,
                meta={"property_type":response.meta.get('property_type')}
            )
            seen = True
        
        if page == 1 or seen:
            url = response.url.replace(f"/{page-1}", f"/{page}")
            yield Request(
                url,
                callback=self.parse,
                meta={"page": page+1, "property_type":response.meta.get('property_type')}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        
        title = response.xpath("normalize-space(//div[contains(@class,'bienTitle')]/h2/text())").get()
        item_loader.add_value("title", title)
        
        external_id = response.xpath("//span[@itemprop='productID']/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(" ")[-1])
        
        address = response.xpath("//p/span[contains(.,'Ville')]/following-sibling::span//text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.strip())
        
        zipcode = response.xpath("//p/span[contains(.,'Code')]/following-sibling::span//text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())
        
        square_meters = response.xpath("//p/span[contains(.,'habitable')]/following-sibling::span//text()").get()
        if square_meters:
            square_meters = square_meters.split("m")[0].strip().replace(",",".")
            item_loader.add_value("square_meters", int(float(square_meters)))
        
        rent = response.xpath("//p/span[contains(.,'Loyer')]/following-sibling::span//text()").get()
        if rent:
            rent = rent.split("€")[0].strip().replace(",",".").replace(" ","")
            item_loader.add_value("rent", int(float(rent)))
        item_loader.add_value("currency", "EUR")
        
        room_count = response.xpath("//p/span[contains(.,'chambre')]/following-sibling::span//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        else:
            room_count = response.xpath("//p/span[contains(.,'pièce')]/following-sibling::span//text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count.strip())
                
        bathroom_count = response.xpath("//p/span[contains(.,'salle')]/following-sibling::span//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        deposit = response.xpath("//p/span[contains(.,'Dépôt')]/following-sibling::span//text()").get()
        if deposit:
            deposit = deposit.split("€")[0].strip().replace(",",".").replace(" ","")
            item_loader.add_value("deposit", int(float(deposit)))
        
        utilities = response.xpath("//p/span[contains(.,'Charges')]/following-sibling::span//text()").get()
        if utilities:
            utilities = utilities.split("€")[0].strip().replace(",",".")
            item_loader.add_value("utilities", int(float(utilities)))
        
        description = "".join(response.xpath("//p[@itemprop='description']//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        
        import dateparser
        if "libre le" in description.lower():
            available_date = description.lower().split("libre le")[1].split(".")[0].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        floor = response.xpath("//p/span[contains(.,'Etage')]/following-sibling::span//text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())
        
        furnished = response.xpath("//p/span[contains(.,'Meublé')]/following-sibling::span//text()[not(contains(.,'Non'))]")
        if furnished:
            item_loader.add_value("furnished", True)
        
        elevator = response.xpath("//p/span[contains(.,'Ascenseur')]/following-sibling::span//text()[not(contains(.,'Non')) and not(contains(.,'NON'))]")
        if elevator:
            item_loader.add_value("elevator", True)
        
        balcony = response.xpath("//p/span[contains(.,'Balcon')]/following-sibling::span//text()[not(contains(.,'Non')) and not(contains(.,'NON'))]")
        if balcony:
            item_loader.add_value("balcony", True)
            
        terrace = response.xpath("//p/span[contains(.,'Terrasse')]/following-sibling::span//text()[not(contains(.,'Non')) and not(contains(.,'NON'))]")
        if terrace:
            item_loader.add_value("terrace", True)
        
        parking = response.xpath("//p/span[contains(.,'parking') or contains(.,'garage')]/following-sibling::span//text()[not(contains(.,'0'))]")
        if parking:
            item_loader.add_value("parking", True)
        
        latitude_longitude = response.xpath("//script[contains(.,'lng')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat :')[1].split(',')[0]
            longitude = latitude_longitude.split('lng:')[1].split('}')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        images = [x for x in response.xpath("//ul[contains(@class,'imageGallery')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_value("landlord_name", "BLOSSAC St HILAIRE IMMOBILIER")
        item_loader.add_value("landlord_email", "contact@agencebsh.com")
        item_loader.add_value("landlord_phone", "33 0 5 49 30 73 73")
        
        yield item_loader.load_item()