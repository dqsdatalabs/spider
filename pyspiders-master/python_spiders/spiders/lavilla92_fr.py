# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser

class MySpider(Spider):
    name = 'lavilla92_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    
    def start_requests(self):

        start_urls = [
            {
                "url" : "https://www.lavilla92.fr/annonces?id_polygon=&localisation_etendu=1&visite_virtuelle=&categorie=location&type_bien=maison&nb_pieces=&surface=&budget=&localisation=&submit=Rechercher",
                "property_type" : "house"
            },
            {
                "url" : "https://www.lavilla92.fr/annonces?id_polygon=&localisation_etendu=1&visite_virtuelle=&categorie=location&type_bien=appartement&nb_pieces=&surface=&budget=&localisation=&submit=Rechercher",
                "property_type" : "apartment"
            },
            

        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//ul/li//p[@class='lien-detail']/a/@href").extract():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")},
            )
        
        next_page = response.xpath("//div[@class='pagelinks-next']/a/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type" : response.meta.get("property_type")}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Lavilla92_PySpider_france") 
        item_loader.add_xpath("title", "//title/text()") 
        item_loader.add_xpath("external_id", "//div[@class='caracteristiques-general']/ul/li[strong[.='Réf. : ']]/text()") 
       
        item_loader.add_xpath("city", "//div[@class='caracteristiques-localisation']/ul/li[strong[contains(.,'Ville')]]/text()") 
        item_loader.add_xpath("zipcode", "//div[@class='caracteristiques-localisation']/ul/li[strong[contains(.,'Code postal')]]/text()") 
        item_loader.add_value("address", "{} {}".format("".join(item_loader.get_collected_values("zipcode")),"".join(item_loader.get_collected_values("city"))))
        item_loader.add_xpath("floor", "//div[@class='caracteristiques-surface']/ul/li[strong[contains(.,'Etage')]]/text()") 
        item_loader.add_xpath("bathroom_count", "//div[@class='caracteristiques-pieces']/ul/li[strong[contains(.,'Nb. de salle d')]]/text()") 
        
        item_loader.add_xpath("room_count", "//div[@class='caracteristiques-pieces']/ul/li[strong[contains(.,'chambres')]]/text() | //div[@class='caracteristiques-pieces']/ul/li[strong[contains(.,'pièces')]]/text()") 
        item_loader.add_xpath("bathroom_count", "//div[@class='caracteristiques-pieces']/ul/li[strong[contains(.,'Nb. de salle de bain')]]/text()") 
        item_loader.add_xpath("energy_label", "substring-before(substring-after(//div[@class='diagnostic_images']/p/img/@src[contains(.,'dpe')],'dpe/'),'/')") 
        item_loader.add_xpath("latitude", "substring-before(substring-after(//script[@type='text/javascript']/text()[contains(.,'var latitude')],'latitude = '),';')") 
        item_loader.add_xpath("longitude", "substring-before(substring-after(//script[@type='text/javascript']/text()[contains(.,'var longitude ')],'longitude = '),';')") 

        available_date=response.xpath("//div[@class='caracteristiques-general']/ul/li[strong[contains(.,'Disponibilité')]]/text()").get()
        if available_date:
            date2 =  available_date.replace("immédiatement","now")
            date_parsed = dateparser.parse(
                date2, date_formats=["%m-%d-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)

        description = " ".join(response.xpath("//div[@class='description']/p//text()").getall())  
        if description:
            item_loader.add_value("description", description.strip())

        meters = " ".join(response.xpath("substring-before(//div[@class='caracteristiques-surface']/ul/li[strong[contains(.,'Surface total')]]/text(),'m²')").getall())  
        if meters:
            s_meters = meters.replace(",",".")
            item_loader.add_value("square_meters", int(float(s_meters))) 


        images = [x for x in response.xpath("//div[@id='photoslider']//li/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)

        rent = "".join(response.xpath("//div[@class='caracteristiques-montant']/ul/li[strong[contains(.,'Loyer')]]/text()").extract())
        if rent:
            price = rent.replace("\xa0","").replace(" ","").strip()
            item_loader.add_value("rent_string", price.strip())

        utilities = "".join(response.xpath("//div[@class='caracteristiques-montant']/ul/li[strong[contains(.,'Charges')]]/text()").extract())
        if utilities:
            uti = utilities.split("/")[0].strip().replace("\xa0","")
            item_loader.add_value("utilities", uti.strip())

        deposit = "".join(response.xpath("//div[@class='caracteristiques-montant']/ul/li[strong[contains(.,'Dépot ')]]/text()").extract())
        if deposit:
            deposit = deposit.strip().replace("\xa0","").replace(" ","")
            item_loader.add_value("deposit", deposit.strip())

        parking = "".join(response.xpath("//div[@class='caracteristiques-garage']/ul/li[strong[contains(.,'parking ') or contains(.,'garage')]]/text()").getall())
        if parking:
            if parking !="0":
                item_loader.add_value("parking",True)
            elif parking == "0":
                item_loader.add_value("parking",False)

        elevator = "".join(response.xpath("//div[@class='caracteristiques-divers']/ul/li[strong[contains(.,'Ascenseur')]]/text()").getall())
        if elevator:
            if "oui" in elevator.lower():
                item_loader.add_value("elevator",True)
            elif "non" in elevator.lower():
                item_loader.add_value("elevator",False)

        balcony = "".join(response.xpath("//div[@class='caracteristiques-divers']/ul/li[strong[contains(.,'Balcon')]]/text()").getall())
        if balcony:
            if "oui" in balcony.lower():
                item_loader.add_value("balcony",True)
            elif "non" in balcony.lower():
                item_loader.add_value("balcony",False)


        furnished = "".join(response.xpath("//div[@class='caracteristiques-divers']/ul/li[strong[contains(.,'Meublé')]]/text()").getall())
        if furnished:
            if "oui" in furnished.lower():
                item_loader.add_value("furnished",True)
            elif "non" in furnished.lower():
                item_loader.add_value("furnished",False)

        item_loader.add_value("landlord_phone", "01-47-33-99-53")
        item_loader.add_value("landlord_name", "LA VILLA 92")
 
        yield item_loader.load_item()

