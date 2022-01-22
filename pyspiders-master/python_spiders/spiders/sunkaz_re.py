# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import re

class MySpider(Spider):
    name = 'sunkaz_re'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url": "https://www.sunkaz.re/louer/?type%5B%5D=Appartement&recherche=&prix%5Bmin%5D=&prix%5Bmax%5D=&surface%5Bmin%5D=&surface%5Bmax%5D=", 
                "property_type": "apartment"
            },
            {
                "url": "https://www.sunkaz.re/louer/?type%5B%5D=Maison&recherche=&prix%5Bmin%5D=&prix%5Bmax%5D=&surface%5Bmin%5D=&surface%5Bmax%5D=", 
                "property_type": "house"
            }
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for url in response.xpath("//div[@class='info-box']//a/@href").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

# 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Sunkaz_PySpider_france")
        
        title = response.xpath("//title/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        
        address = response.xpath("//div[@class='heading-area']/strong[contains(@class,'subtitle')]/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address)
        
        external_id = response.xpath("//strong[@class='vendor-code']/text()").get()
        if external_id:
            print(external_id)
            item_loader.add_value("external_id", external_id.split(" ")[1])
        
        square_meters = response.xpath("//strong[contains(.,'habitable')]/following-sibling::span/text()").get()
        if square_meters:
            square_meters = square_meters.split("m")[0].strip().replace(",",".")
            item_loader.add_value("square_meters", int(float(square_meters)))
        
        room_count = response.xpath("//strong[contains(.,'Chambre')]/span[1]/text()").get()
        room = response.xpath("//strong[contains(.,'piece')]/following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        elif room:
            item_loader.add_value("room_count", room)
        
        bathroom_count = response.xpath("//strong[contains(.,'salle')]/span[1]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        floor = response.xpath("//strong[contains(.,'étage')]/following-sibling::span/text()").get()
        if floor:
            item_loader.add_value("floor", floor.split("/")[0])
        
        rent = response.xpath("//strong[contains(.,'Loyer')]/following-sibling::span/text()").get()
        if rent:
            item_loader.add_value("rent", rent.split("€")[0].strip().replace(" ",""))
        item_loader.add_value("currency", "EUR")
        
        deposit = response.xpath("//strong[contains(.,'Depot')]/following-sibling::span/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split("€")[0].strip())
        
        utilities = response.xpath("//strong[contains(.,'Charge')]/following-sibling::span/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split("€")[0].strip())
        
        swimming_pool = response.xpath("//strong[contains(.,'Piscine')]/following-sibling::span/text()[.!='0']").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)
        
        furnished = response.xpath("//strong[contains(.,'Meublé')]/following-sibling::text()[contains(.,'Oui')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        import dateparser
        available_date = response.xpath("//p/strong/text()").get()
        if available_date:
            match = re.search(r'(\d+/\d+/\d+)', available_date.replace(" ",""))
            if match:
                newformat = dateparser.parse(match.group(1), languages=['en']).strftime("%Y-%m-%d")
                item_loader.add_value("available_date", newformat)

        lat_lng = response.xpath("//div/@data-polygon").get()
        if lat_lng:
            try:
                lng = lat_lng.split("[[")[1].split(",")[0]
                lat = lat_lng.split("[[")[1].split(",")[1].split("]")[0]
                item_loader.add_value("latitude", lat)
                item_loader.add_value("longitude", lng)
            except:
                pass
        
        description = " ".join(response.xpath("//span[@itemprop='description']//p//text()").getall())
        if description:
            desc = re.sub('\s{2,}', ' ', description.strip())
            item_loader.add_value("description", desc.strip())
        
        images = [response.urljoin(x) for x in response.xpath("//div[@id='mosaic-slideshow']//@href").getall()]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_value("landlord_name", "Sunkaz Immobilier")
        item_loader.add_value("landlord_phone", "0262 490 490")
        item_loader.add_value("landlord_email", "contact@sunkaz.re")
        
        yield item_loader.load_item()