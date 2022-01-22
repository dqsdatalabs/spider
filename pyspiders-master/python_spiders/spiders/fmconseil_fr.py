# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import math

class MySpider(Spider):
    name = 'fmconseil_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="Fmconseil_PySpider_france_fr"
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
            {"url": "http://www.fmconseil.fr/a-louer/1"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                             callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        seen = False
 
        for item in response.xpath("//div[@class='bienTitle']"):
            follow_url = response.urljoin(item.xpath("./h1/a/@href").get())
            property_type = item.xpath("./h2/text()").get()
            seen = True
            if property_type.strip().lower().startswith('maison'):
                yield Request(follow_url, callback=self.populate_item, meta={"property_type": "house"})
            elif property_type.strip().lower().startswith('appartement'):
                yield Request(follow_url, callback=self.populate_item, meta={"property_type": "apartment"})
            elif property_type.strip().lower().startswith('studio'):
                yield Request(follow_url, callback=self.populate_item, meta={"property_type": "studio"})

        if page == 2 or seen:
            url = f"http://www.fmconseil.fr/a-louer/{page}"
            yield Request(url, callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", self.external_source)
        commercial= "".join(response.xpath("//p[@class='data']/span[.='Type de transac']/following-sibling::span/text()[contains(.,'commercial')]").extract())
        if commercial:
            pass
        else:
            item_loader.add_xpath("title", "//div[@class='themTitle']/h1/text()")
            url = response.url
            item_loader.add_value("external_link", response.url)
            if "suevres" not in url and "terrain" not in url:
                item_loader.add_value("property_type", response.meta.get('property_type'))

                external_id = "".join(response.xpath("normalize-space(//li[@class='ref']/text())").extract())
                if external_id:
                    item_loader.add_value("external_id",external_id.replace("Ref","").strip())

                bathroom_count = "".join(response.xpath("//p[span[contains(. ,'salle de bains')]]/span[@class='valueInfos ']/text()").extract())
                if bathroom_count:
                    item_loader.add_value("bathroom_count", bathroom_count.strip())
                else:
                    bathroom_count = "".join(response.xpath("//p[span[contains(. ,\"Nb de salle d'eau\")]]/span[@class='valueInfos ']/text()").extract())
                    if bathroom_count:
                        item_loader.add_value("bathroom_count", bathroom_count.strip())

                price = response.xpath("normalize-space(//p[span[contains(. ,'Loyer') or contains(.,'Prix')]]/span[@class='valueInfos ']/text())").get()
                if price:
                    item_loader.add_value("rent", price.replace(" ",""))
                item_loader.add_value("currency", "EUR")

                deposit = response.xpath("//p[span[. ='Dépôt de garantie TTC']]/span[@class='valueInfos ']/text()[not(contains(.,'Non'))]").extract_first()
                if deposit:
                    deposit = deposit.strip()
                    item_loader.add_value("deposit", deposit.split("€")[0])

                utilities = response.xpath("normalize-space(//p[span[contains(. ,'Charges')]]/span[@class='valueInfos ']/text())").extract_first()
                if utilities:
                    item_loader.add_value("utilities", utilities.split("€")[0])

                square = response.xpath("normalize-space(//p[span[contains(. ,'Surface habitable')]]/span[@class='valueInfos ']/text())").get()
                if square:
                    square = square.split("m²")[0]
                    if "," in square:
                        square = square.replace(",",".")
                    square = math.ceil(float(square))
                    item_loader.add_value("square_meters", str(square))

                images = [response.urljoin(x)for x in response.xpath("//ul[@class='imageGallery  loading']/li/img/@src").extract()]
                if images:
                        item_loader.add_value("images", images)

                item_loader.add_xpath("room_count","normalize-space(//p[span[contains(. ,'Nombre de pièces')]]/span[@class='valueInfos ']/text())")
                floor = response.xpath("normalize-space(//p[span[contains(. ,'Etage')]]/span[@class='valueInfos ']/text())").get()
                if floor:
                    if "er" in floor:
                        floor = floor.split("er")[0]
                    item_loader.add_value("floor", floor)

                desc = "".join(response.xpath("//p[@itemprop='description']/text()").extract())
                if desc:
                    item_loader.add_value("description", desc.strip())
                
                if "lave-vaisselle" in desc.lower():
                    item_loader.add_value("dishwasher", True)

                if "piscine" in desc.lower():
                    item_loader.add_value("swimming_pool", True)
                
                terrace = "".join(response.xpath("//p[span[contains(. ,'Terrasse')]]/span[@class='valueInfos ']/text()").extract())
                if terrace:
                    if terrace.strip().lower() == 'non':
                        item_loader.add_value("terrace", False)
                    elif terrace.strip().lower() == 'oui':
                        item_loader.add_value("terrace", True)

                balcony = "".join(response.xpath("//p[span[contains(. ,'Balcon')]]/span[@class='valueInfos ']/text()").extract())
                if balcony:
                    if balcony.strip().lower() == 'non':
                        item_loader.add_value("balcony", False)
                    elif balcony.strip().lower() == 'oui':
                        item_loader.add_value("balcony", True)

                parking = "".join(response.xpath("//p[span[contains(. ,'parking') or contains(. ,'garage')]]/span[@class='valueInfos ']/text()").extract())
                if parking:
                    if parking.strip() != "0": 
                        item_loader.add_value("parking", True)

                elevator = "".join(response.xpath("//p[span[contains(. ,'Ascenseur')]]/span[@class='valueInfos ']/text()").extract())
                if elevator:
                    if elevator.strip().lower() == 'non':
                        item_loader.add_value("elevator", False)
                    elif elevator.strip().lower() == 'oui':
                        item_loader.add_value("elevator", True)

                furnished = "".join(response.xpath("//p[span[contains(. ,'Meublé')]]/span[@class='valueInfos ']/text()").extract())
                if furnished:
                    if furnished.strip().lower() == 'non':
                        item_loader.add_value("furnished", False)
                    elif furnished.strip().lower() == 'oui':
                        item_loader.add_value("furnished", True)

                zipcode = response.xpath("normalize-space(//p[span[contains(. ,'Code postal')]]/span[@class='valueInfos ']/text())").get()
                city = response.xpath("normalize-space(//p[span[contains(. ,'Ville')]]/span[@class='valueInfos ']/text())").get()
                item_loader.add_value("zipcode", zipcode)
                item_loader.add_value("city", city)
                item_loader.add_value("address", city + ' (' + zipcode + ')')

                latitude_longitude=response.xpath("//script[contains(.,'lat')]/text()").get()
                if latitude_longitude:
                    latitude=latitude_longitude.split("lat :")[1].split(",")[0].strip()
                    longitude=latitude_longitude.split("lng:")[1].split("}")[0].strip()
                    item_loader.add_value("latitude", latitude)
                    item_loader.add_value("longitude", longitude)
                
                item_loader.add_value("landlord_phone", "02 54 46 45 14")
                item_loader.add_value("landlord_email", "contact@fmconseil.fr")
                item_loader.add_value("landlord_name", "Fm Conseil")

                yield item_loader.load_item()