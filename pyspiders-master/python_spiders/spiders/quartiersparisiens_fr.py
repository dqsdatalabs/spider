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
    name = 'quartiersparisiens_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    post_url = "https://www.quartiersparisiens.fr/recherche/"
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
        formdata = {
            "data[Search][offredem]": "2",
            "data[Search][idtype][]": "2",
            "data[Search][prixmax]": "",
            "data[Search][prixmin]": "",
            "data[Search][surfmax]": "",
            "data[Search][surfmin]": "",
            "data[Search][piecesmax]": "",
            "data[Search][piecesmin]": "",
            "data[Search][NO_DOSSIER]": "",
        }
        yield FormRequest(self.post_url,
                        callback=self.parse,
                        formdata=formdata,
                        dont_filter=True,
                        meta={'property_type': "apartment"})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        for url in response.xpath("//div[@class='links-group__wrapper']/div/@data-url").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        if response.xpath("//a[@class='pagination__link'][contains(.,'suivante')]/@href").get():     
            p_url = f"https://www.quartiersparisiens.fr/recherche/{page}"
            yield Request(
                p_url,
                callback=self.parse,
                dont_filter=True,
                meta={"page":page+1, "property_type":response.meta["property_type"]})

                
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_source","Quartiersparisiens_PySpider_france")
        
        title = response.xpath("//h1[@class='title__content']/span/text()").get()
        item_loader.add_value("title", title)
        
        rent = response.xpath("//div[@class='main-info__price']/span[1]/text()").get()
        if rent:
            price = rent.split("€")[0].replace(" ","").replace(",",".")
            item_loader.add_value("rent", int(float(price)))
        item_loader.add_value("currency", "EUR")
        
        item_loader.add_xpath("external_id", "//span[@class='detail-1__reference-number']/text()")
        
        zipcode = response.xpath("//div/span[contains(.,'Code')]/following-sibling::span/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode)
        
        address = response.xpath("//div/span[contains(.,'Ville')]/following-sibling::span/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address)
            
        square_meters = response.xpath("//div/span[contains(.,'habitable')]/following-sibling::span/text()").get()
        if square_meters:
            square_meters = square_meters.split("m")[0].strip().replace(",",".")
            item_loader.add_value("square_meters", int(float(square_meters)))  
        
        room_count = response.xpath("//div/span[contains(.,'chambre')]/following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//div/span[contains(.,'pièce')]/following-sibling::span/text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//div[@class='option'][contains(.,'salle')]/span/text()").get()
        item_loader.add_value("bathroom_count", bathroom_count)
        
        floor = response.xpath("//div/span[contains(.,'Etage')]/following-sibling::span/text()").get()
        if floor:
            item_loader.add_value("floor", floor)
        
        description = " ".join(response.xpath("//div[@class='detail-1__text']//p//text()| //div[contains(@class,'secondarycolor')]//text()").getall())
        if description:
            description = re.sub('\s{2,}', ' ', description.strip())
            item_loader.add_value("description", description)
        
        images = [x for x in response.xpath("//div[contains(@class,'swiper-wrappe')]//@href[contains(.,'jpg')]").getall()]
        if images:
            item_loader.add_value("images", images)
        
        deposit = response.xpath("//div/span[contains(.,'garantie')]/following-sibling::span/text()").get()
        if deposit:
            deposit = deposit.split("€")[0].replace(" ","").replace(",",".")
            item_loader.add_value("deposit", int(float(deposit)))
        
        utilities = response.xpath("//div/span[contains(.,'Charges')]/following-sibling::span/text()").get()
        if utilities:
            utilities = utilities.split("€")[0].strip().replace(",",".")
            item_loader.add_value("utilities", int(float(utilities)))
        
        furnished = response.xpath("//div/span[contains(.,'Meublé')]/following-sibling::span/text()").get()
        if furnished and "oui" in furnished.lower():
            item_loader.add_value("furnished", True)
        
        elevator = response.xpath("//div/span[contains(.,'Ascenseur')]/following-sibling::span/text()").get()
        if elevator and "oui" in elevator.lower():
            item_loader.add_value("elevator", True)
        
        balcony = response.xpath("//div/span[contains(.,'Balcon')]/following-sibling::span/text()").get()
        if balcony and "oui" in balcony.lower():
            item_loader.add_value("balcony", True)
        
        terrace = response.xpath("//div/span[contains(.,'Terrasse')]/following-sibling::span/text()").get()
        if terrace and "oui" in terrace.lower():
            item_loader.add_value("terrace", True)
        
        item_loader.add_xpath("latitude", "//div/@data-lat")
        item_loader.add_xpath("longitude", "//div/@data-lng")
        
        item_loader.add_value("landlord_name", "QUARTIERS PARISIENS")
        item_loader.add_value("landlord_phone", "06 69 41 12 07")
        item_loader.add_value("landlord_email", "contact@quartiersparisiens.fr")
        
        yield item_loader.load_item()