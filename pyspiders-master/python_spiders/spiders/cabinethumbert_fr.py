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
    name = 'cabinethumbert_fr'
    execution_type='testing'
    country='france'
    locale='fr'

    post_url = "https://www.cabinethumbert.fr/resultat.asp"
    def start_requests(self):
        formdata = {
            "IdTypeOffre": "1",
            "Ville": "",
            "Trace": "",
            "TexteTypeBien": "",
            "CategorieAppartement": "on",
            "CritereLocalDansVenteLocationhorizontale": "1",
            "TexteCombien": "",
            "BudgetMini": "",
            "BudgetMaxi": "",
            "SurfaceMini": "",
            "SurfaceMaxi": "",
            "TextePlusDeCriteres": "",
            "CritereParReference": "",
            "Commercial": "",
        }

        yield FormRequest(
            url=self.post_url,
            callback=self.parse,
            formdata=formdata,
            meta={
                "property_type":"apartment",
            }
        )


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//a[contains(@class,'lien resultattitre')]/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True

        if page == 2 or seen:
            p_url = f"https://www.cabinethumbert.fr/miniresultat.asp?Page={page}"
            yield Request(p_url, callback=self.parse, meta={"property_type":response.meta["property_type"], "page":page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        item_loader.add_value("external_source", "Cabinethumbert_PySpider_france")
        item_loader.add_value("external_id", response.url.split("ref-")[1].split(".")[0])

        title = " ".join(response.xpath("//h1[contains(@class,'location fichebien')]//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        desc = " ".join(response.xpath("//div[contains(@id,'FicheDescriptifDebutTexte')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        address = response.xpath("//h2[contains(@class,'location')][contains(.,'Adresse')]/following-sibling::p//text()").get()
        if address:
            item_loader.add_value("address", address)
            city = address.split(",")[0].split(" ")
            city1 = ""
            for x in city:
                if not x.isdigit():
                    city1 = city1+x+" "
            item_loader.add_value("city", city1.strip())

        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//h2[contains(@class,'location')][contains(.,'Disponibilité')]/following-sibling::p//text()").getall())
        if available_date:
            if "immédiate" in available_date.lower():
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            else:
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        square_meters = "".join(response.xpath("//img[contains(@src,'surface')]/parent::div//text()").getall())
        if square_meters:
            square_meters = square_meters.split("Surface de")[1].split("m2")[0].strip().replace(",",".")
            item_loader.add_value("square_meters", int(float(square_meters)))
        
        floor = response.xpath("//img[contains(@src,'immeuble')]/parent::div//text()").get()
        if floor:
            floor = floor.strip().split(" ")[0].replace("ème","").replace("er","")
            item_loader.add_value("floor", floor)
        
        room_count = response.xpath("//img[contains(@src,'chambres')]/parent::div//text()").get()
        if room_count:
            room_count = room_count.strip().split(" ")[0]
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//img[contains(@src,'pieces')]/parent::div//text()").get()
            if room_count:
                room_count = room_count.strip().split(" ")[0]
                item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//img[contains(@src,'sdb')]/parent::div//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip().split(" ")[0]
            item_loader.add_value("bathroom_count", bathroom_count)

        rent = response.xpath("//li[contains(.,'loyer')]//span[contains(@class,'valeur')]//text()").get()
        if rent:
            rent = rent.split("€")[0].strip().replace("\u00a0","")
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency", "EUR")
        
        utilities = response.xpath("//li[contains(.,'des charges locatives')]//span[contains(@class,'valeur')]//text()").get()
        if utilities:
            utilities = utilities.split("€")[0].strip()
            item_loader.add_value("utilities",utilities)

        deposit = response.xpath("//li[contains(.,'dépôt de garantie')]//span[contains(@class,'valeur')]//text()").get()
        if deposit:
            deposit = deposit.split("€")[0].strip().replace("\u00a0","")
            item_loader.add_value("deposit",deposit)

        elevator = response.xpath("//img[contains(@src,'ascenseur')]/parent::div//text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        terrace = response.xpath("//img[contains(@src,'terrasse')]/parent::div//text()").get()
        if terrace:
            item_loader.add_value("terrace", True)

        parking = response.xpath("//img[contains(@src,'stationnement')]/parent::div//text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        balcony = response.xpath("//img[contains(@src,'balcon')]/parent::div//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)

        furnished = response.xpath("//span[contains(.,'meublé')]//following-sibling::span//text()[contains(.,'oui')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        energy_label = response.xpath("//h2[contains(.,'Consommation énergetique')]//following-sibling::div//text()[not(contains(.,'Vierge'))]").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)

        images = [x for x in response.xpath("//div[contains(@id,'photofichebien')]//@data-src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        latitude = response.xpath("//input[contains(@id,'MinLatitude')]//@value").get()
        if latitude:
            item_loader.add_value("latitude", latitude)
        
        longitude = response.xpath("//input[contains(@id,'MinLongitude')]//@value").get()
        if longitude:
            item_loader.add_value("longitude", longitude)
        
        landlord_name = response.xpath("//a[contains(@href,'agence')]//strong//text()[2]").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)
        
        landlord_phone = response.xpath("//a[contains(@href,'tel')]//text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)

        yield item_loader.load_item()