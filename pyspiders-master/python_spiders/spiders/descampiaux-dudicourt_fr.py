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
import dateparser
class MySpider(Spider):
    name = 'descampiaux-dudicourt_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="DescampiauxDudicourt_PySpider_france"
    custom_settings = {"HTTPCACHE_ENABLED": False}
    formdata={
        "IdTypeOffre": "1",
        "Ville": "",
        "Trace": "",
        "TexteTypeBien": "",
        "CategorieMaison": "on",
        "CategorieAppartement": "on",
        "CritereLocalDansVenteLocationhorizontale": "1",
        "TexteCombien": "",
        "BudgetMini": "",
        "BudgetMaxi": "",
        "SurfaceMini": "",
        "SurfaceMaxi": "",
        "TextePlusDeCriteres":"" ,
        "CritereParReference": "",
        "Commercial": ""
    }
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.descampiaux-dudicourt.fr/resultat.asp",
                ],
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield FormRequest(item,formdata=self.formdata,
                            callback=self.parse)
    # 1. FOLLOWING
    def parse(self, response):
        border=10
        page = response.meta.get('page', 2)
        seen = False
        for item in  response.xpath("//a[@class='lien resultattitre']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        if border:
            if page<=int(border)+3:
                if page==2 or seen:
                    nextpage=f"https://www.descampiaux-dudicourt.fr/resultat.asp?Page={page}"
                    if nextpage:
                        yield Request(nextpage, callback=self.parse,meta={"page":page+1})
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source",self.external_source)
        item_loader.add_value("external_link", response.url)

        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)
        property_type=response.xpath("//h1[@class='location fichebien']/span/text()").get()
        if property_type and "appartement" in property_type.lower():
            item_loader.add_value("property_type","apartment")
        if property_type and "maison" in property_type.lower():
            item_loader.add_value("property_type","house")
        if property_type and "studio" in property_type.lower():
            item_loader.add_value("property_type","studio")
        adres=response.xpath("//h2[@class='location']/following-sibling::p/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        square_meters=response.xpath("//div/sup[.='2']/preceding-sibling::text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("Surface de")[-1].split("m")[0].split(",")[0].strip())
        room_count=response.xpath("//img[contains(@src,'pieces')]/following-sibling::text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.split("pi")[0].strip())
        bathroom_count=response.xpath("//img[contains(@src,'nbsdb')]/following-sibling::text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.split("salle")[0].strip())
        deposit=response.xpath("//span[.='dépôt de garantie :']/following-sibling::span/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.split("€")[0])
        utilities=response.xpath("//span[.='honoraires charge locataire TTC :']/following-sibling::span/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split("€")[0].split(",")[0].replace("\xa0","").strip())
        energy_label=response.xpath("//div[@class='FicheDescriptifDiagNiveau1']/text()").get()
        if energy_label:
            item_loader.add_value("energy_label",energy_label)
        rent=response.xpath("//span[.='loyer mensuel charges comprises :']/following-sibling::span/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[0].replace("\xa0","").strip())
        item_loader.add_value("currency","GBP")
        description=response.xpath("//h2[.='Description']/following-sibling::p/text()").getall()
        if description:
            item_loader.add_value("description",description)
        images=[x for x in response.xpath("//img[contains(@src,'descampiaux') and contains(@src,'photo-type')]/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        item_loader.add_value("landlord_name","Citya Descampiaux")
        item_loader.add_value("landlord_phone","03.20.14.52.00")
        yield item_loader.load_item()