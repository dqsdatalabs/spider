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
import re


class MySpider(Spider):
    name = 'iluro_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Iluro_PySpider_france_fr"

    def start_requests(self):
        start_urls = [
            {"url": "https://iluro.fr/index.php?action=list&menuid=10100&ctypmandatmulti%5B%5D=lm&ctypmandatmulti%5B%5D=l&ctypmetamulti%5B%5D=mai&mprixmin=&mprixmax=&cbien=&qsurfterrain=&qsurfhab=&orderby=bien.mprix+asc&search=", "property_type": "house"},
            {"url": "https://iluro.fr/index.php?action=list&menuid=10100&ctypmandatmulti%5B%5D=lm&ctypmandatmulti%5B%5D=l&ctypmetamulti%5B%5D=appt&mprixmin=&mprixmax=&cbien=&qsurfterrain=&qsurfhab=&orderby=bien.mprix+asc&search=", "property_type": "apartment"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                             callback=self.parse,
                             meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 1)
        
        seen = False
        for follow_url in response.xpath("//div[@class='products']/div[contains(@class,'eight')]//a[@class='more-details']/@href").extract():
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            seen = True
        
        if response.meta.get("property_type") == "apartment":
            if page == 1 or seen:
                url = f"https://iluro.fr/index.php?page={page}&action=list&menuid=10100&ctypmandatmulti%5B%5D=lm&ctypmandatmulti%5B%5D=l&ctypmetamulti%5B%5D=appt&mprixmin=&mprixmax=&cbien=&qsurfterrain=&qsurfhab=&orderby=bien.mprix+asc&search=#toplist"
                yield Request(url, callback=self.parse, meta={"page": page+1, 'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", self.external_source)

        title = response.xpath("//title//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))

        external_id = "".join(response.xpath("//p[@class='alignright']/text()[contains(.,'Réf')]").extract())
        if external_id:
            item_loader.add_value("external_id",external_id.split(":")[1])

        parking = response.xpath("//div[@id='desc']/h4/text()[contains(.,'GARAGE') or contains(.,'PARKING')]").get()
        if parking:
            item_loader.add_value("parking", True)

        price = response.xpath("normalize-space(//p[@class='alignleft']/text())").extract_first()
        if price:
            item_loader.add_value("rent", price.split(":")[1].split("€")[0].strip())
            item_loader.add_value("currency", "EUR")

        utilities = response.xpath("//p[contains(.,'Charges')]/text()[.!='Charges mensuelles: ']").extract_first()
        if utilities:
            utilities = utilities.split(":")[1].replace("€","")
            if utilities:
                item_loader.add_value("utilities", utilities.replace("€",""))
        else:
            utilities = response.xpath("//li[contains(.,'Charges')]/text()").get()
            if utilities:
                item_loader.add_value("utilities", "".join(filter(str.isnumeric, utilities.split(':')[-1].split('€')[0].strip())))

        available_date =response.xpath("//p[contains(.,'Date disponibilité:')]/text()").extract_first()
        if available_date:
            date_parsed = dateparser.parse(available_date.split(":")[1].strip(), date_formats=["%d %B %Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d")) 

        room_count = "".join(response.xpath("//ul[@class='list-4 color']/li/text()[contains(.,'Chambre')]").extract())
        if room_count:
            room_count=re.findall("\d+",room_count)
            item_loader.add_value("room_count", room_count)
        else:
            room_count = "".join(response.xpath("//ul[@class='list-4 color']/li/text()[contains(.,'Pièces')]").extract())
            if room_count:
                room_count=re.findall("\d+",room_count)
                item_loader.add_value("room_count", room_count)
            else:
                 room_studio = response.xpath("//div/h2/text()[contains(.,'STUDIO')]").extract_first()
                 if room_studio:
                     item_loader.add_value("room_count", "1")

        bathroom_count=response.xpath("//ul[@class='list-4 color']/li/text()[contains(.,'Salle')]").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split("Salle")[0].strip())
        else:
            bathroom_count = response.xpath("//p[contains(.,\"Salle d'eau\")]").getall()
            if bathroom_count:
                item_loader.add_value("bathroom_count", len(bathroom_count))
        
        square = "".join(response.xpath("//ul[@class='list-4 color']/li/text()[contains(.,'Surface')]").extract())
        if square:
            item_loader.add_value("square_meters", square.split(":")[1].strip().split("m²")[0].strip())

        desc = "".join(response.xpath("//div[@id='desc']/p/text()").extract())
        item_loader.add_value("description", desc.strip())
        
        floor=False
        if "ETAGE" in desc:
            floor=desc.split("ETAGE")[0].replace("EME","").replace("ER","").strip().split(" ")[-1]
        elif "\u00e9tage" in desc:
            floor=desc.split("\u00e9tage")[0].replace("ième","").replace("ème","").strip().split(" ")[-1]
        if floor and floor.isdigit():
            item_loader.add_value("floor", floor)

        deposit=response.xpath("//div/p[contains(.,'Caution')]/text()").get()
        if deposit and "." in deposit:
            item_loader.add_value("deposit", deposit.split(":")[1].split(".")[0].strip())
        elif deposit:
            item_loader.add_value("deposit", deposit.split(":")[1].split("€")[0].strip())

        furnished=response.xpath("//div[@id='desc']//text()[contains(.,' meublé') and not(contains(.,'non meublé'))]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        images = [response.urljoin(x)for x in response.xpath("//div[@class='fotorama']/img/@src").extract()]
        if images:
                item_loader.add_value("images", images)


        terrace = "".join(response.xpath("//ul[@class='list-4 color']/li/text()[contains(.,'Terrain:')]").extract())
        if terrace:
            item_loader.add_value("terrace", True)

        terrace = "".join(response.xpath("//p[contains(.,'Balcon ')]/text()").extract())
        if terrace:
            item_loader.add_value("balcony", True)

        address = "".join(response.xpath("//p[@class='alignright']/text()[contains(.,'Code postal')]").extract())
        if address:
            item_loader.add_value("address", address.strip().split("Code postal :")[1].strip())
            item_loader.add_value("city", address.split("Localité(s)")[1].replace(": ","").strip())
            item_loader.add_value("zipcode", address.split(":")[1].split("-")[0].strip())

        energy_label=response.xpath("//div/h3[contains(.,'DPE')]/parent::div//p[contains(.,'Consommation')]/text()").get()
        if energy_label:
            energy_label=energy_label.split("Classe")[1].strip()
            item_loader.add_value("energy_label", energy_label)
        
        item_loader.add_value("landlord_phone", "05.59.39.03.78")
        item_loader.add_value("landlord_email", "agence-iluro@wanadoo.fr")
        item_loader.add_value("landlord_name", "Iluro")

                
        yield item_loader.load_item()