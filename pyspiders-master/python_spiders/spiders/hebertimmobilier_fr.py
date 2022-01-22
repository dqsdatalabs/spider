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
    name = 'hebertimmobilier_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url": "https://www.hebertimmobilier.fr/recherche,incl_recherche_prestige_ajax.htm?surfacemin=Min&surfacemax=Max&surf_terrainmin=Min&surf_terrainmax=Max&px_loyermin=Min&px_loyermax=Max&idqfix=1&idtt=1&pres=prestige&idpays=250&lang=fr&idtypebien=1&tri=d_dt_crea&_=1615895236372",
                "property_type": "apartment"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(
                url=url.get('url'),
                callback=self.parse,
                meta={'property_type': url.get('property_type')}
            )

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@class='span8']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(
                follow_url,
                callback=self.populate_item,
                meta={'property_type': response.meta.get('property_type')}
            )
            
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Hebertimmobilier_PySpider_france")
        title = " ".join(response.xpath("//h1//text()").getall())
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
   
        floor = response.xpath("//li[div[.='Etage']]/div[2]/text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())
        external_id = response.xpath("//div[@class='bloc-detail-reference']/span/text()[contains(.,'Référence')]").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[-1].strip())
        city = response.xpath("//h1//text()[last()]").get()
        if city:            
            item_loader.add_value("address", city)
            item_loader.add_value("city", city.split("(")[0].strip())
            item_loader.add_value("zipcode", city.split("(")[1].split(")")[0].strip())
         
        available_date = response.xpath("//p[@class='dt-dispo']//text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.split(":")[-1].strip(), date_formats=["%d/%m/%Y"], languages=['fr'])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        rent = "".join(response.xpath("//div[span[@itemprop='price']]//text()").getall())
        if rent:
            item_loader.add_value("rent_string",rent.replace("\xa0",""))
        
        description = "".join(response.xpath("//p[@itemprop='description']//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        bathroom_count = response.xpath('//li[div[.="Salle d\'eau" or .="Salle de bain"]]/div[2]/text()').get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        room_count = response.xpath("//li[div[contains(.,'Chambre')]]/div[2]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            item_loader.add_xpath("room_count", "//li[div[contains(.,'Pièce')]]/div[2]/text()")
     
        square_meters = response.xpath("//li[div[.='Surface']]/div[2]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", int(float(square_meters.split("m")[0].replace(",",".").strip())))
        deposit = response.xpath("//div[@class='row-fluid']/strong/text()[contains(.,'Dépôt de garantie :')]").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split(":")[-1].replace(" ","").strip())
        utilities = response.xpath("//ul/li//text()[contains(.,'Provisions pour charges')]").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split(":")[-1].replace(" ","").strip())
        parking = response.xpath("//li[div[.='Parking']]/div[2]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        balcony = response.xpath("//li[div[.='Balcon']]/div[2]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        elevator = response.xpath("//li[contains(.,'Ascenseur')]/text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
        terrace = response.xpath("//li[div[.='Terrasse']]/div[2]/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        furnished = response.xpath("//li[div[.='Meublé']]/div[2]/text()").get()
        if furnished:
            if "non" in furnished.lower():
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)
        energy_label = response.xpath("//div[p[.='Consommations énergétiques']]//div[contains(@class,'dpe-bloc-lettre')]/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.strip())
    
        images = [response.urljoin(x) for x in response.xpath("//div[@id='slider']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "HEBERT IMMOBILIER")
        item_loader.add_value("landlord_phone", "04 50 47 50 50")
        item_loader.add_value("landlord_email", "contact@hebertimmobilier.immo")
        yield item_loader.load_item()