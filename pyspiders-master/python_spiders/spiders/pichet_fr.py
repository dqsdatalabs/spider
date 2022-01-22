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
    name = 'pichet_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.pichet.fr/rent-search?rent%5Baddress%5D=&rent%5Bregion%5D=&rent%5Bdepartment%5D=&rent%5Bcity%5D=&rent%5BzipCode%5D=&rent%5Bradius%5D=&rent%5BminRent%5D=&rent%5BmaxRent%5D=&rent%5BminLivingArea%5D=&rent%5BmaxLivingArea%5D=&rent%5Btypes%5D%5B%5D=appartement&length=2000",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.pichet.fr/rent-search?rent%5Baddress%5D=&rent%5Bregion%5D=&rent%5Bdepartment%5D=&rent%5Bcity%5D=&rent%5BzipCode%5D=&rent%5Bradius%5D=&rent%5BminRent%5D=&rent%5BmaxRent%5D=&rent%5BminLivingArea%5D=&rent%5BmaxLivingArea%5D=&rent%5Btypes%5D%5B%5D=maison&length=2000",
                    
                    ],
                "property_type": "house"
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
        data = json.loads(response.body)
        for item in data["searchResults"]:
            follow_url = response.urljoin(item["url"])
            meta_data = {
                "property_type": response.meta.get('property_type'),
                "lat": str(item["latitude"]),
                "lng": str(item["longitude"]),
                "city": item["city"],
                "id": item["reference"],
            }
            yield Request(
                follow_url,
                callback=self.populate_item,
                meta=meta_data,
            )   
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("latitude", response.meta.get('lat'))
        item_loader.add_value("longitude", response.meta.get('lng'))
        item_loader.add_value("city", response.meta.get('city'))
        # item_loader.add_value("address", response.meta.get('city'))
        item_loader.add_value("external_id", response.meta.get('id'))
        item_loader.add_value("external_source", "Pichet_PySpider_france")

        title = response.xpath("//h1/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        
        address = response.xpath("//script[contains(.,'window.address')]/text()").get()
        if address:
            address = address.split("window.address =")[1].split('"')[1]
            item_loader.add_value("address", address)
            
            zipcode = ""
            for x in address.split(" "):
                if x.isdigit():
                    zipcode = x
            
            item_loader.add_value("zipcode", zipcode)
        
        rent = response.xpath("//span[@class='price']/text()").get()
        if rent:
            price = rent.split("€")[0].replace(" ","")
            item_loader.add_value("rent", price)
        item_loader.add_value("currency", "EUR")

        square_meters = response.xpath("//span[contains(.,'Surface')]/following-sibling::span/text()").get()
        if square_meters:
            square_meters = square_meters.split("m")[0].strip()
            item_loader.add_value("square_meters", int(float(square_meters)))
        
        energy_label = response.xpath("//img/@src[contains(.,'energy-consumption-')]").get()
        if energy_label:
            energy_label = energy_label.split("-")[-1].split(".")[0]
            item_loader.add_value("energy_label", energy_label.upper())
        
        deposit = response.xpath("//span[contains(.,'Dépôt')]/following-sibling::span/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split("€")[0].strip())
        
        utilities = response.xpath("//span[contains(.,'Dont')]/following-sibling::span/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split("€")[0].strip())
        
        external_id = response.xpath("//span[contains(.,'Référence')]/following-sibling::span/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)
        
        floor = response.xpath("//span[contains(.,'Étage')]/following-sibling::span/text()").get()
        if floor:
            item_loader.add_value("floor", floor)
        
        import dateparser
        available_date = response.xpath("//span[contains(.,'Disponibil')]/following-sibling::span/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        parking = response.xpath("//span[contains(.,'Prestations')]/following-sibling::ul/li/text()[contains(.,'Parking')]").get()
        if parking:
            item_loader.add_value("parking", True)
        
        elevator = response.xpath("//span[contains(.,'Prestations')]/following-sibling::ul/li/text()[contains(.,'Ascenseur')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        terrace = response.xpath("//span[contains(.,'Prestations')]/following-sibling::ul/li/text()[contains(.,'Terrasse')]").get()
        if terrace:
            item_loader.add_value("terrace", True)
         
        description = " ".join(response.xpath("//div[@class='description']//text()").getall())
        if description:
            description = re.sub('\s{2,}', ' ', description.strip())
            item_loader.add_value("description", description)
        
        if "chambre" in description.lower():
            room_count = description.lower().split("chambre")[0].strip().split(" ")[-1]
            if room_count.isdigit():
                item_loader.add_value("room_count", int(room_count))
        
        images = [response.urljoin(x) for x in response.xpath("//div[@class='top-image']//picture//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "Pichet Group")
        item_loader.add_value("landlord_phone", "09 69 32 18 95")
        item_loader.add_value("landlord_email", "dpo@pichet.com")
        
        yield item_loader.load_item()