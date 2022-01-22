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
    name = 'clesev_fr'
    execution_type='testing'
    country='france'
    locale='fr'
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
        url = "https://www.clesev.fr/recherche/"
        headers = {
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1',
            'Origin': 'https://www.clesev.fr',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Referer': 'https://www.clesev.fr/recherche/',
            'Accept-Language': 'tr,en;q=0.9',
        }
        start_urls = [
            {
                "formdata" : {
                    'data[Search][offredem]': '2', 
                    'data[Search][idtype][]': '2'
                    },
                "property_type" : "apartment",
            },
        ]
        for item in start_urls:
            yield FormRequest(url, formdata=item["formdata"], headers=headers, dont_filter=True, callback=self.parse, meta={'property_type': item["property_type"]})

    def parse(self, response):

        page = response.meta.get("page", 2)
        seen = False

        for item in response.xpath("//div[@class='card col']/a/@href").getall():
            seen = True
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type": response.meta["property_type"]})
        
        if page == 2 or seen:
            yield Request(f"https://www.clesev.fr/recherche/{page}", callback=self.parse, meta={"property_type": response.meta["property_type"], "page": page + 1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        if "-test.html" in response.url:
            return
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Clesev_PySpider_france")      
        item_loader.add_xpath("title", "//h1/span/text()")
        external_id = response.xpath("//span[@class='labelprix ref']/following-sibling::text()[1]").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
        room_count = response.xpath("//tr[th[contains(.,'chambre')]]/th[2]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split("Chambre")[0])
        else:
            item_loader.add_xpath("room_count", "//tr[th[contains(.,'pièces')]]/th[2]/text()")

        item_loader.add_xpath("bathroom_count", "//tr[th[contains(.,'Nb de salle d')]]/th[2]/text()")
        item_loader.add_xpath("zipcode", "//tr[th='Code postal']/th[2]/text()")
        item_loader.add_xpath("city", "//tr[th='Ville']/th[2]/text()")
        item_loader.add_xpath("address", "//tr[th='Ville']/th[2]/text()")
        item_loader.add_xpath("floor", "//tr[th='Etage']/th[2]/text()")        
        square_meters = response.xpath("//tr[th='Surface habitable (m²)']/th[2]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", int(float(square_meters.split(" m")[0].strip().replace(",","."))))
      
        description = " ".join(response.xpath("//p[@itemprop='description']//text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())
     
        furnished = response.xpath("//tr[th[.='Meublé']]/th[2]/text()[.!='Non renseigné']").get()
        if furnished:
            if furnished.lower() =="non":
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)
        elevator = response.xpath("//tr[th[.='Ascenseur']]/th[2]/text()").get()
        if elevator:
            if elevator.lower() =="non":
                item_loader.add_value("elevator", False)
            else:
                item_loader.add_value("elevator", True)
        parking = response.xpath("//tr[th[.='Nombre de garage' or .='Nombre de parking']]/th[2]/text() ").get()
        if parking:
            if parking.lower() =="0":
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)
        images = [response.urljoin(x) for x in response.xpath("//ul[contains(@class,'imageGallery')]/li//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        script_map = response.xpath("//script[contains(.,'center: { lat :')]/text()").get()
        if script_map:
            item_loader.add_value("latitude", script_map.split("center: { lat :")[1].split(",")[0].strip())
            item_loader.add_value("longitude", script_map.split("center: { lat :")[1].split("lng:")[1].split("}")[0].strip())
        rent = response.xpath("//tr[th[.='Loyer CC* / mois']]/th[2]/text() ").get()
        if rent:
            item_loader.add_value("rent_string", rent.replace(" ",""))

        deposit = response.xpath("//tr[th[.='Dépôt de garantie TTC']]/th[2]/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split("€")[0])

        utilities = response.xpath("//tr[th[contains(.,'Charges locatives')]]/th[2]/text() ").get()
        if utilities:
            item_loader.add_value("utilities", int(float(utilities.replace(" ","").split("€")[0].strip())))
        item_loader.add_value("landlord_name", "Clesev")
        item_loader.add_value("landlord_phone", "04 72 52 23 45")
        yield item_loader.load_item()