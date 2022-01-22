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
    name = 'agencelebail_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url": "http://www.agencelebail.com/recherche,incl_recherche_prestige_ajax.htm?surfacemin=Min&surfacemax=Max&surf_terrainmin=Min&surf_terrainmax=Max&px_loyermin=Min&px_loyermax=Max&idqfix=1&idtt=1&pres=prestige&idtypebien=1&lang=fr&_=1614259153754",
                "property_type": "apartment",
                "type":"1",
            },
	        {
                "url": "http://www.agencelebail.com/recherche,incl_recherche_prestige_ajax.htm?surfacemin=Min&surfacemax=Max&surf_terrainmin=Min&surf_terrainmax=Max&pxmin=Min&pxmax=Max&idqfix=1&idtt=1&pres=prestige&idtypebien=2&lang=fr&tri=d_dt_crea&_=1614259280165",
                "property_type": "house",
                "type":"2",
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "type":url.get('type')})

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@class='row-fluid']//div[@class='span8']//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
        
        page = response.xpath("//li[@class='refine-page']//text()").get()
        if page:
            for i in range(2,int(page)+1):
                url = f"http://www.agencelebail.com/recherche,incl_recherche_prestige_ajax.htm?_=1614259153754&idqfix=1&idtt=1&idtypebien={response.meta.get('type')}&lang=fr&pres=prestige&px_loyermax=Max&px_loyermin=Min&surf_terrainmax=Max&surf_terrainmin=Min&surfacemax=Max&surfacemin=Min&annlistepg={i}"
                yield Request(url, callback=self.parse, meta={"type": response.meta.get('type'), "property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Agencelebail_PySpider_france")   

        title = " ".join(response.xpath("//h1/text()").getall()) 
        if title:
            item_loader.add_value("title", re.sub("\s{2,}", " ", title))
        item_loader.add_xpath("external_id", "substring-after(//div[@class='bloc-detail-reference']/span[contains(.,'Référence')]/text(),': ')")
        room_count = response.xpath("//li[div[.='Chambre']]/div[2]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            item_loader.add_xpath("room_count", "//li[div[.='Pièces' or .='Pièce']]/div[2]/text()")
        bathroom_count = response.xpath("//li[div[contains(.,'Salle d')]]/div[2]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        floor = response.xpath("//li[div[.='Etage']]/div[2]/text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())
        address = response.xpath("//h1/text()[last()]").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split("(")[0].strip())
            item_loader.add_value("zipcode", address.split("(")[1].split(")")[0].strip())
    
        parking = response.xpath("//li[div[contains(.,'Parking')]]/div[2]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        elevator = response.xpath("//li[div[.='Ascenseur']]/div[2]/text()").get()
        if elevator:
            if "non" in elevator.lower():
                item_loader.add_value("elevator", False)
            elif "oui" in elevator.lower():
                item_loader.add_value("elevator", True)
   
        square_meters = response.xpath("//li[div[.='Surface']]/div[2]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0].split(",")[0])
        energy_label = response.xpath("//div[@class='diagramme'][img[contains(@src,'/diag_dpe')]]/p[contains(@class,'diagLettre')]/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)
      
        description = " ".join(response.xpath("//p[@itemprop='description']//text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())
       
        images = [response.urljoin(x) for x in response.xpath("//div[@id='slider']/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)

        utilities = response.xpath("//li[contains(.,'Charges :')]/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split(":")[-1])
        
        deposit = response.xpath("//strong[contains(.,'Dépôt de garantie :')]/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split(":")[-1].replace(" ",""))
        rent = response.xpath("//span[@itemprop='price']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.replace("\xa0",""))
        item_loader.add_value("currency", "EUR")

        item_loader.add_value("landlord_name", "AGENCE LE BAIL")
        item_loader.add_value("landlord_phone", "01 30 38 23 91")
        item_loader.add_value("landlord_email", "location@agencelebail.fr")

        lat_lng = response.xpath("//script[contains(.,',LATITUDE: ') and contains(.,',LONGITUDE: ')]/text()").get()
        if lat_lng:
            item_loader.add_value("latitude", lat_lng.split("ANNONCE: {")[-1].split(',LATITUDE: "')[-1].split('"')[0].strip())
            item_loader.add_value("longitude", lat_lng.split("ANNONCE: {")[-1].split(',LONGITUDE: "')[-1].split('"')[0].strip())

        yield item_loader.load_item()