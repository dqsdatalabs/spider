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
    name = 'sextiusmirabeau_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://www.sextiusmirabeau.com/recherche,incl_recherche_basic_ajax.htm?cp=13&surfacemin=Min&surfacemax=Max&surf_terrainmin=Min&surf_terrainmax=Max&px_loyermin=Min&px_loyermax=Max&idqfix=1&idtt=1&ANNLISTEpg={}&pres=basic&lang=fr&idtypebien=1&tri=d_dt_crea&_=1613396870987",
                ],
                "property_type" : "apartment",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(1),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base": item})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[@class='span9']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True

        if page == 2 or seen:
            base_url = response.meta.get("base")
            p_url = base_url.format(page)
            yield Request(p_url, callback=self.parse, meta={"property_type":response.meta["property_type"], "base":base_url, "page":page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        status = " ".join(response.xpath("//p[@itemprop='description']//text()[contains(.,'ommerciale')]").getall())
        if status:
            return
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        item_loader.add_value("external_source", "Sextiusmirabeau_PySpider_france")
        
        title = " ".join(response.xpath("//h1//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        address = response.xpath("//h1//text()[2]").get()
        if address:
            item_loader.add_value("address", address.split("(")[0].strip())
            item_loader.add_value("city", address.split("(")[0].strip())
            item_loader.add_value("zipcode", address.split("(")[1].split(")")[0])

        rent = response.xpath("//span[@itemprop='price']/text()").get()
        if rent:
            item_loader.add_value("rent", rent.replace("\r\n","").replace("\u00a0",""))
        item_loader.add_value("currency", "EUR")
        
        square_meters = response.xpath("//li[contains(.,'Surface')]/div[2]/text()").get()
        if square_meters:
            square_meters = square_meters.strip().replace("\xa0"," ").split(" ")[0].replace(",",".")
            item_loader.add_value("square_meters", int(float(square_meters)))
        
        room_count = response.xpath("//div[contains(text(),'pièce')]/text()").get()
        if room_count:
            room_count = room_count.split()[0]
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//li[contains(.,'Chambres')]/div[2]/text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.xpath("//li[contains(.,'Salle')]/div[2]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        floor = response.xpath("//li[contains(.,'Etage')]/div[2]/text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())
            
        deposit = response.xpath("//strong[contains(.,'garantie :')]/text()[not(contains(.,'N/A'))]").get()
        if deposit:
            deposit = deposit.split(":")[1].split("€")[0].strip().replace(",",".")
            item_loader.add_value("deposit", int(float(deposit)))

        utilities = response.xpath("//li[contains(.,'Charges')]/text()").get()
        if utilities:
            utilities = utilities.split(":")[1].split("€")[0].strip()
            item_loader.add_value("utilities", utilities)
        
        energy_label = response.xpath("normalize-space(//div/p[contains(.,'énergétiques')]/..//div[contains(@class,'dpe-bloc-lettre')]/text())").get()
        item_loader.add_value("energy_label", energy_label)
        
        external_id = response.xpath("//div[contains(@class,'reference')]/span[contains(.,'Référence')]/text()").get()
        if external_id:
            external_id = external_id.split(":")[1].strip()
            item_loader.add_value("external_id", external_id)

        balcony = response.xpath("normalize-space(//li[contains(.,'Balcon')]/div[2]/text())").get()
        if balcony and balcony !='0':
            item_loader.add_value("balcony", True)
        
        parking = response.xpath("normalize-space(//li[contains(.,'Parking')]/div[2]/text())").get()
        if parking and parking !='0':
            item_loader.add_value("parking", True)
        
        terrace = response.xpath("//li[contains(.,'Terrasse')]/div[2]/text()[contains(.,'oui')]").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        elevator = response.xpath("//li[contains(.,'Ascenseur')]/div[2]/text()[contains(.,'oui')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        furnished = response.xpath("//li[contains(.,'Meublé')]/div[2]/text()[contains(.,'oui')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        from datetime import datetime
        available_date = "".join(response.xpath("//p[@itemprop='description']//text()[contains(.,'Disponible immédiatement')]").getall())
        if available_date:
            item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            
        desc = " ".join(response.xpath("//p[@itemprop='description']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        images = [x for x in response.xpath("//div[@id='slider']//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_value("landlord_name", "IMMOBILIERE SEXTIUS MIRABEAU")
        item_loader.add_value("landlord_phone", "33442912777")
        item_loader.add_value("landlord_email", "accueil@sextiusmirabeau.com")
        
        yield item_loader.load_item()