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
    name = 'plaisantimmobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'

    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Content-Type": "application/x-www-form-urlencoded",
        "Origin": "http://www.plaisant-immobilier.com",
    }
    urls = []
    def start_requests(self):
        start_urls = [
            {"url": "http://www.plaisant-immobilier.com/locations_r.aspx?c=1&tb=2&nbp=0&p=0&l=0&r=", "property_type": "apartment"},
	        {"url": "http://www.plaisant-immobilier.com/locations_r.aspx?c=1&tb=1&nbp=0&p=0&l=0&r=", "property_type": "house"},
            
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')
                        })

    # 1. FOLLOWING
    def parse(self, response):
        property_type = response.meta.get("property_type")
        page = response.meta.get('page', 1)
        
        seen = False
        for item in response.xpath("//div[@id='CPHContenu_mod_location_r1_LVListe_itemPlaceholderContainer']/article//a[@class='color-2']/@href").extract():
            follow_url = response.urljoin(item)
            if follow_url not in self.urls:
                self.urls.append(follow_url)
                yield Request(follow_url, callback=self.populate_item, meta={"property_type":property_type})
                seen = True
            else:
                seen = False           
        
        if seen:
            radscript = response.xpath("//input[@id='RadScriptManager1_TSM']/@value").get()
            viewstate = response.xpath("//input[@id='__VIEWSTATE']/@value").get()
            generator = response.xpath("//input[@id='__VIEWSTATEGENERATOR']/@value").get()
            validation = response.xpath("//input[@id='__EVENTVALIDATION']/@value").get()
            p_type = response.xpath("//select[@name='ctl00$CPHContenu$mod_location_r1$mod_moteur1$DDLType']/option[@selected]/@value").get()
        
            data = {
                "RadScriptManager1_TSM": f"{radscript}",
                "__EVENTTARGET": f"ctl00$CPHContenu$mod_location_r1$LVListe$RadDataPager1$ctl01$ctl0{page}",
                "__EVENTARGUMENT": "",
                "__LASTFOCUS": "",
                "__VIEWSTATE": f"{viewstate}",
                "__VIEWSTATEGENERATOR": f"{generator}",
                "__VIEWSTATEENCRYPTED": "",
                "__EVENTVALIDATION": f"{validation}",
                "ctl00_RSSFiche_ClientState": "",
                "ctl00_CPHContenu_mod_location_r1_LVListe_RadDataPager1_ClientState": "",
                "ctl00$CPHContenu$mod_location_r1$mod_moteur1$DDLAchatVente": "1",
                "ctl00$CPHContenu$mod_location_r1$mod_moteur1$DDLType": f"{p_type}",
                "ctl00$CPHContenu$mod_location_r1$mod_moteur1$DDLPiece": "0",
                "ctl00$CPHContenu$mod_location_r1$mod_moteur1$DDLLoyer": "0",
                "ctl00$CPHContenu$mod_location_r1$mod_moteur1$DDLLocalisation": "0",
                "ctl00$CPHContenu$mod_location_r1$mod_moteur1$TBref": "",
                "ctl00_CPHContenu_mod_location_r1_ZoomPhoto_ClientState": "",
                "ctl00_CPHContenu_mod_location_r1_RadWindowManager1_ClientState": ""
            }

            yield FormRequest(
                response.url,
                formdata=data,
                headers=self.headers,
                callback=self.parse,
                meta={"page": page+1, "property_type":property_type}
            )
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source","Plaisant_Immobilier_PySpider_france")
        item_loader.add_xpath("title", "//title/text()")

        external_id = "".join(response.xpath("//div[@class='container_12']/div/p/span[contains(.,'Réf :')]/text()").getall())
        if external_id:
            item_loader.add_value("external_id", external_id.replace("·","").strip())

        room2 = ""
        room_count = "".join(response.xpath("//p[@class='color-8']/span[@id='CPHContenu_lblChambre']/text()").getall())
        if room_count:
            room = room_count.replace("·","").strip()
            if  room == "0":
                room_count = "".join(response.xpath("//p[@class='color-8']/span[@id='CPHContenu_lblPiece']/text()").getall())
                if room_count:
                    room2 = room_count.replace("·","").strip()
            else:
                room2 = room
        item_loader.add_value("room_count", room2) 

        square = "".join(response.xpath("//p[@class='color-8']/span[@id='CPHContenu_lblSurfaceF']/strong/text()").getall())
        if square:
            item_loader.add_value("square_meters", square.split("m²")[0].strip())

        rent = "".join(response.xpath("substring-before(//p[@class='prix']/span[@id='CPHContenu_lblPrix']/text(),'/')").getall())
        if rent:
            price = rent.replace(" ","").strip().split("€")[0]
            item_loader.add_value("rent", int(float(price)))

        item_loader.add_value("currency", "EUR")

        utilities = "".join(response.xpath("//p[@class='color-8']/span[@id='CPHContenu_lblChargeF']/strong/text()").getall())
        if utilities:
            item_loader.add_value("utilities", utilities)

        deposit = "".join(response.xpath("//p[@class='color-8']/span[@id='CPHContenu_lblDepot']/strong/text()").getall())
        if deposit:
            item_loader.add_value("deposit", deposit.strip())

        floor = "".join(response.xpath("//p[@class='color-8']/span[@id='CPHContenu_lblEtage']/strong/text()").getall())
        if floor:
            item_loader.add_value("floor", floor.strip())

        address = "".join(response.xpath("//div[@class='grid_9']/h2/span[@id='CPHContenu_lblLocalisation']/text()").getall())
        if address:
            item_loader.add_value("address", address.strip())
        
        city = "".join(response.xpath("//p[@class='color-8']/span[@id='CPHContenu_lblTitre']/text()").getall())
        if city:
            item_loader.add_value("city", city.strip())

        description = " ".join(response.xpath("//p[@class='color-8']/span[@id='CPHContenu_lblDescription']/text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.strip())
        
        images = [ response.urljoin(x) for x in response.xpath("//div[@class='pro_vignette_photo']/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)

        elevator = " ".join(response.xpath("//p[@class='color-8']/span[@id='CPHContenu_lblAscenseur']/strong/text()").getall()).strip()   
        if elevator:
            if elevator.lower() == "non":
                item_loader.add_value("elevator", False)
            elif elevator == "oui":
                item_loader.add_value("elevator", True)

        item_loader.add_value("landlord_phone", "04 91 13 31 60")
        item_loader.add_value("landlord_name", "J & M PLAISANT IMMOBILIER")
        item_loader.add_value("landlord_email", "info@plaisant-immobilier.com")


        yield item_loader.load_item()