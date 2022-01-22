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
    name = 'immoravi_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://www.immoravi.com/_actions.php?token=&marche=1&transaction=2&sous_type=1&agence=18039&action=show_annonces_continuous_scroll&offset=0",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "http://www.immoravi.com/_actions.php?token=&marche=1&transaction=2&sous_type=3&agence=18039&action=show_annonces_continuous_scroll&offset=0",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item, callback=self.parse, meta={'property_type': url.get('property_type')})

    def parse(self, response):

        page = response.meta.get("page", 1)
        seen = False

        for item in response.xpath("//h3/a/@href").getall():
            seen = True
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type": response.meta["property_type"]})
        
        if page == 1 or seen:
            follow_url = response.url.replace("&offset=" + str(page - 1), "&offset=" + str(page))
            yield Request(follow_url, callback=self.parse, meta={"property_type": response.meta["property_type"], "page": page + 1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
 
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Immoravi_PySpider_france")       
        
        title = " ".join(response.xpath("//h1/span/text() ").getall()) 
        if title:
            item_loader.add_value("title", title.strip())
        item_loader.add_xpath("external_id", "substring-after(//h2/span[contains(.,'Réf: ')]/text(),'Réf: ')")
        room_count = response.xpath("//li/span[contains(.,'Chambre')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split("Chambre")[0])
        else:
            item_loader.add_xpath("room_count", "substring-before(//li/span[contains(.,'Pièce')]/text(),'Pièce')")

        bathroom_count = response.xpath("//li/span[contains(.,'Salle de bains') or contains(.,'Salle d')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split("Salle")[0])
        address = response.xpath("//h2/span[contains(.,'Réf: ')]/text()").get()
        if address:
            address = address.split("/")[0].strip()
            item_loader.add_value("address", address)
            item_loader.add_value("zipcode", address.split(" - ")[1])
            item_loader.add_value("city", address.split(" - ")[0])
        
        square_meters = response.xpath("//li/span[contains(.,'Surface de')]/text()").get()
        if square_meters:
            square_meters = square_meters.split("Surface de")[1].split("m")[0].strip()
            item_loader.add_value("square_meters", int(float(square_meters.replace(",","."))))
        parking = response.xpath("//li/span[.='Parking' or .='Garage']/text()").get()
        if parking:
            item_loader.add_value("parking", True)
    
        terrace = response.xpath("//li/span[.='Terrasse']/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        balcony = response.xpath("//li/span[.='Balcon']/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        elevator = response.xpath("//li/span[.='Ascenseur']/text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
        energy_label = response.xpath("//li//span[contains(.,'DPE :')]/text()[not(contains(.,'Vierge') or contains(.,'Non'))]").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split("DPE :")[1].split("(")[0].strip())
     
        description = " ".join(response.xpath("//div[contains(@class,'lo-details-intro')]//div[contains(@class,'lo-box-content')]/p[1]//text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())
     
        images = [response.urljoin(x) for x in response.xpath("//div[contains(@class,'images-full-slider')]//a/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        else:
            images = [response.urljoin(x) for x in response.xpath("//div[@class='module-images-slick-gallerie']//a/img/@src").getall()]
            if images:
                item_loader.add_value("images", images)    
        rent = response.xpath("//span[@class='price']/span/text()").get()
        if rent:
            item_loader.add_value("rent_string", rent.replace("\xa0",".").replace(" ",""))
        deposit = response.xpath("//p[@class='details-descriptif-prix-honoraires']//text()[contains(.,'Dépôt de garantie :')]").get()
        if deposit:
            deposit = deposit.split(":")[1].split("€")[0].replace(" ","").strip()
            item_loader.add_value("deposit", int(float(deposit.replace(",","."))))
        utilities = response.xpath("//p[@class='details-descriptif-prix-loyer-charges']//text()[contains(.,'€ de charges ')]").get()
        if utilities:
            utilities = utilities.split(":")[1].split("€ de charges ")[0].replace(" ","").strip()
            item_loader.add_value("utilities", int(float(utilities.replace(",","."))))
        item_loader.add_value("landlord_name", "IMMOBILIERE DE LA RAVINELLE")
        item_loader.add_value("landlord_phone", "03.83.35.00.47")
        yield item_loader.load_item()