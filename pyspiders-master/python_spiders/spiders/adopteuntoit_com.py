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
    name = 'adopteuntoit_com'
    execution_type='testing'
    country='france' 
    locale='fr'
    start_urls = ['https://www.adopteuntoit.com/wp-content/themes/adopteTheme/searchEngine/listBien.xml']  # LEVEL 1
    
    headers = {
        "accept": "application/xml, text/xml, */*; q=0.01",
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "tr,en;q=0.9,tr-TR;q=0.8,en-US;q=0.7,es;q=0.6,fr;q=0.5,nl;q=0.4",
        "cookie": "_ga=GA1.2.1081449019.1615986572; PHPSESSID=1b57ed246a8be587d8c3671761251084; prixMax=false; nbChambre=false; nbPiece=false; surface=false; _gid=GA1.2.736079759.1616572529; neuf=false; userCookiePol=true; appartement=true; villa=false; louer=false; acheter=true; _gat_gtag_UA_136937503_1=1",
        "origin": "https://www.adopteuntoit.com",
        "referer": "https://www.adopteuntoit.com/nos-annonces-immobilieres-a-la-reunion/",
        "sec-ch-ua": '"Google Chrome";v="89", "Chromium";v="89", ";Not A Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36",
        "x-requested-with": "XMLHttpRequest",
    }
    
    def start_requests(self):
        yield Request(
            url=self.start_urls[0],
            headers=self.headers,
            callback=self.parse
        )

    # 1. FOLLOWING
    def parse(self, response):
        
        sel = Selector(text=response.body, type='xml')
        for item in sel.xpath("//BIEN"):
            sale = item.xpath("./VENTE").get()
            apartment = item.xpath("./APPARTEMENT").get()
            if not sale and apartment:
                f_id = item.xpath("./INFO_GENERALES/AFF_NUM/text()").get()
                f_url = f"https://www.adopteuntoit.com/fiche-dinformation-du-bien/?ref={f_id}"
        
                yield Request(f_url, callback=self.populate_item, meta={"property_type": "apartment", "item":item})
 
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Adopteuntoit_PySpider_france")
        item_loader.add_value("external_id", response.url.split("=")[-1])
        
        import dateparser
        item = response.meta.get('item')
        available_date = item.xpath("./INFO_GENERALES/LIBRELE/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        title = item.xpath("./INTITULE/FR/text()").get()
        item_loader.add_value("title", title)
        
        address = item.xpath(".//ADRESSE/text()").get()
        city = item.xpath(".//VILLE/text()").get()
        if address or city:
            item_loader.add_value("address", f"{address.strip()} {city.strip()}")
            item_loader.add_value("city", city.strip())
        
        zipcode = item.xpath(".//CODE_POSTAL/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())
        
        rent = item.xpath("./LOCATION/LOYER/text()").get()
        item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")
        
        deposit = item.xpath("./LOCATION/GARANTIE_REVENTE/text()").get()
        if deposit and not "Non" in deposit:
            item_loader.add_value("deposit", deposit)
        
        # room_count = item.xpath(".//NBRE_CHAMBRES/text()").get()
        # room_count=response.xpath("//p[@class='PieceBien']/span/text()").get()
        # room_count=re.findall("\d+",room_count)
        room_count = item.xpath(".//NBRE_PIECES/text()").get()
        item_loader.add_value("room_count", room_count)
        
        square_meters = item.xpath(".//SURFACE_HABITABLE/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", int(float(square_meters)))
        
        bathroom_count = item.xpath(".//NBRE_SALLE_BAIN/text()").get()
        if bathroom_count and bathroom_count!='0':
            item_loader.add_value("bathroom_count", bathroom_count)
        
        parking = item.xpath(".//NBRE_PARKING/text()").get()
        if parking and parking!='0':
            item_loader.add_value("parking", True)
        
        furnished = item.xpath(".//MEUBLE/text()").get()
        if furnished and furnished!='0':
            item_loader.add_value("furnished", True)
        
        elevator = item.xpath(".//ASCENSEUR/text()").get()
        if elevator and elevator!='0':
            item_loader.add_value("elevator", True)
        
        floor = item.xpath(".//NUM_ETAGE/text()").get()
        item_loader.add_value("floor", floor)
        
        latitude = item.xpath(".//LATITUDE/text()").get()
        item_loader.add_value("latitude", latitude)

        longitude = item.xpath(".//LONGITUDE/text()").get()
        item_loader.add_value("longitude", longitude)
        
        description = item.xpath("./COMMENTAIRES/FR/text()").get()
        item_loader.add_value("description", description)
        
        images = [x for x in item.xpath("./IMAGES/IMG/text()").getall()]
        item_loader.add_value("images", images)
        
        item_loader.add_value("landlord_name", "Adopte Un Toit")
        item_loader.add_value("landlord_phone", "0262 70 20 00")
        item_loader.add_value("landlord_email", "contact@adopteuntoit.com")
        
        yield item_loader.load_item()