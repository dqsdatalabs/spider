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
    name = 'bellavita_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Bellavita_PySpider_france_fr'
    def start_requests(self):
        start_urls = [
            {"url": "https://www.bellavita.fr/resultats.php?page=annonces.php&transac=location&type=appartement&budget_mini=0&budget_maxi=3000&surface_mini=0&surface_maxi=200&nb_pieces=0&ville=&submit=Rechercher", "property_type": "apartment"},
	        {"url": "https://www.bellavita.fr/resultats.php?page=annonces.php&transac=location&type=maison&budget_mini=0&budget_maxi=3000&surface_mini=0&surface_maxi=200&nb_pieces=0&ville=&submit=Rechercher", "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                            })

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@id='results']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Bellavita_PySpider_"+ self.country + "_" + self.locale)

        title = response.xpath("//h1/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.xpath("substring-after(//span[@class='texte16']/text(), ':')").get().split())
   
        description = response.xpath("//h3[contains(.,'DESCRIPTION')]/following-sibling::text()[1]").get()
        if description:
            description = re.sub('\s{2,}', ' ', description.strip())
            item_loader.add_value("description", description)
        
        address = response.xpath("//span[contains(.,'Situation')]/strong//text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.strip().split(" ")[-1])
            item_loader.add_value("zipcode", address.strip().split(" ")[0])
        
        # property_type = response.xpath("//span[contains(.,'Bien')]/strong//text()").get()
        item_loader.add_value("property_type", response.meta.get('property_type'))
        
        square_meters = response.xpath("//span[contains(.,'habitable')]/strong//text()").get()
        if square_meters:
            square_meters = square_meters.strip().split(" ")[0]
        item_loader.add_value("square_meters", square_meters)
        
        room_count = response.xpath("//text()[contains(.,'chambre(')]/following-sibling::b[1]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        else:
            room_count = response.xpath("//text()[contains(.,'pièce')]/following-sibling::b[1]/text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.xpath("//text()[contains(.,'Salle d')]/following-sibling::b[1]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        

        images = [response.urljoin(x.replace("/vignettes","")) for x in response.xpath("//div[@id='diaporama2']/div/img/@src").getall()]
        if images:
            img = list(set(images))
            item_loader.add_value("images", img)
            # item_loader.add_value("external_images_count", str(len(images)))
        
        price = response.xpath("//span[contains(.,'Loyer')]/strong//text()").get()
        if price:
            item_loader.add_value("rent_string", price.replace(" ",""))
        # item_loader.add_value("currency", "EUR")
        
        utilities = response.xpath("//span[contains(.,'Charges')]/strong//text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split(" ")[0].strip())

        elevator = response.xpath("//text()[contains(.,'Ascenseur')]/following-sibling::b[1]/text()").get()
        if elevator:
            if elevator.lower().strip() != "non":
                item_loader.add_value("elevator", True)
            else:
                item_loader.add_value("elevator", False)

        energy_label = response.xpath("//text()[contains(.,'énergétique')]/following-sibling::strong/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split(" ")[0])

        floor = response.xpath("//text()[contains(.,'Etage')]/following-sibling::b[1]/text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())

        balcony = response.xpath("//text()[contains(.,'Balcon')]/following-sibling::b[1]/text()").get()
        if balcony:
            if balcony.lower().strip() != "non":
                item_loader.add_value("balcony", True)
            else:
                item_loader.add_value("balcony", False)
        
        item_loader.add_value("landlord_email", "accueil@bellavita.fr")
        item_loader.add_value("landlord_name", "Bellavita")
        

        yield item_loader.load_item()