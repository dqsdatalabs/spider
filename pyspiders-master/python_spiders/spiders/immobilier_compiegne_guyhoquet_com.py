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
    name = 'immobilier_compiegne_guyhoquet_com'
    execution_type='testing'
    country='france'
    locale='fr'
    headers = {
        'content-type': "application/json; charset=utf-8",
        'accept': "*/*",
        'accept-encoding': "gzip, deflate, br",
        'accept-language': "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
        'referer': "https://compiegne.guy-hoquet.com/biens/result",
        'sec-fetch-dest': "empty",
        'sec-fetch-mode': "cors",
        'sec-fetch-site': "same-origin",
        'x-csrf-token': "dcMUKcNrpGbQVLZwSNN0ecZ9Jjnx7vT9Hr0Psgqk",
        'x-requested-with': "XMLHttpRequest",
        'cache-control': "no-cache",
    }
    
    def start_requests(self):
        start_urls = [
           
            {"url": "https://compiegne.guy-hoquet.com/biens/result?1&templates%5B%5D=properties&p=1&t=&filters%5B10%5D%5B%5D=2&filters%5B30%5D%5B0%5D%5B%5D=appartement&with_markers=false&_=1608888458167", "property_type": "apartment"},
	        {"url": "https://compiegne.guy-hoquet.com/biens/result?1&templates%5B%5D=properties&p=1&t=&filters%5B10%5D%5B%5D=2&filters%5B30%5D%5B0%5D%5B%5D=maison&with_markers=false&_=1608888458169", "property_type": "house"},
        ] 
        for url in start_urls:
            yield Request(url=url.get('url'),
                            headers=self.headers,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        
        seen = False
        data = json.loads(response.body)
        data_html = data["templates"]["properties"]
        sel = Selector(text=data_html, type="html")

        data_url = sel.xpath("//div[contains(@class,'resultat-item')]/a/@href").extract()
        for item in data_url:
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item ,dont_filter=True, meta={'property_type': response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            p_url = response.url.split("&p=")[0] + f"&p={page}" + "&t=" + response.url.split("&t=")[1]
            yield Request(
                p_url,
                callback=self.parse,
                headers=self.headers,
                meta={'property_type': response.meta.get('property_type'), "page":page+1}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        desc="".join(response.xpath("//div[@class='no-desk']//span[@class='description-more']/text()").getall())
        if "pascale rocheron" in desc.lower():
            return

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Immobilier_Compiegne_Guyhoquet_PySpider_france")
        item_loader.add_xpath("title", "//div/h1/text()")
        rent="".join(response.xpath("//div[@class='price']/text()").getall())
        if rent:
            item_loader.add_value("rent_string", rent.replace(" ",""))
        
        square_meters="".join(response.xpath("//div[@id='intro']//i[@class='ico scale']/following-sibling::div/text()").getall())
        if square_meters:
            meters = square_meters.split('m²')[0].strip()
            item_loader.add_value("square_meters", int(float(meters)))
        
        room_count="".join(response.xpath("//div[@id='intro']//i[contains(@class,'ico king_bed')]/following-sibling::div/text()").getall())
        if room_count:
            item_loader.add_value("room_count", room_count.strip().split(' ')[0])
        else:
            room_count="".join(response.xpath("//div[@id='intro']//i[contains(@class,'ico room')]/following-sibling::div/text()").getall())
            if room_count:
                item_loader.add_value("room_count", room_count.strip().split(' ')[0])
        bathroom_count="".join(response.xpath("//div[@class='biens-list']//div/i[contains(@class,'ico bath')]/parent::div//text()").getall())
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip().split(' ')[0])
        elif not bathroom_count:
            bathroom_count="".join(response.xpath("//div[contains(@class,'v-desk')]//div[@class='biens-list']//div/i[contains(@class,'ico shower')]/parent::div//text()").getall())
            if bathroom_count:
                item_loader.add_value("bathroom_count", bathroom_count.strip().split(' ')[0])
        
        address = response.xpath("//div[@class='add']/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city"," ".join(address.split(" ")[:-1]))
            item_loader.add_value("zipcode",address.split(" ")[-1] )
        
        latitude_longitude = response.xpath("//script[@type='text/javascript'][contains(.,'Lat')]/text()").get()
        if latitude_longitude:
                latitude = latitude_longitude.split(" Lat = '")[1].split("';")[0]
                longitude = latitude_longitude.split("Lng = '")[1].split("';")[0]
                item_loader.add_value("longitude", longitude)
                item_loader.add_value("latitude", latitude)
           
        external_id=response.xpath("//div[@class='code']/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[-1].strip())

        desc="".join(response.xpath("//div[@class='no-desk']//span[@class='description-more']/text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
        
        floor=response.xpath("//div[@class='horaires-item']/div/div[contains(.,'Etage')]//following-sibling::div/text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())
            
        images=[x for x in response.xpath("//div[@class='de-biens-slider']/div/@href").getall()]
        if images:
            item_loader.add_value("images", images)
      
        utilities=response.xpath("//div[@class='horaires-item']/div/div[contains(.,'Provision sur charges')]//following-sibling::div/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split('€')[0].strip())
        
        deposit = response.xpath("//div[@class='horaires-item']/div/div[contains(.,'de garantie')]//following-sibling::div/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split('€')[0].strip())
        
        furnished = response.xpath("//div[@class='horaires-item']/div/div[contains(.,'Meubl')]//following-sibling::div/text()").get()
        if furnished:
            if "Oui" in furnished:
                item_loader.add_value("furnished", True)
            else:
                item_loader.add_value("furnished", False)
        terrace = response.xpath("//div[contains(text(),'Nombre de terrasse')]/following-sibling::div/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        parking="".join(response.xpath("//div[@class='biens-list']//div/i[contains(@class,'parking')]/parent::div//text()").getall())
        if parking:
            item_loader.add_value("parking", True)
        balcony="".join(response.xpath("//div[@class='horaires-item']/div/div[contains(.,'Nombre de balcons')]//following-sibling::div/text()").getall())
        if balcony:
            item_loader.add_value("balcony", True)
        energy_label = response.xpath("//div[@class='horaires-item']//div[contains(.,'Conso Energ')]/following-sibling::div/text()").get()
        if energy_label:
            energy_label = energy_label.strip().split(" ")[0].split(".")[0]
            item_loader.add_value("energy_label", energy_label_calculate(energy_label))
        item_loader.add_value("landlord_name", "Guy Hoquet COMPIEGNE")
        item_loader.add_value("landlord_phone", "0344300285")
        yield item_loader.load_item()


def energy_label_calculate(energy_number):
    energy_number = int(energy_number)
    energy_label = ""
    if energy_number <= 50:
        energy_label = "A"
    elif energy_number > 50 and energy_number <= 90:
        energy_label = "B"
    elif energy_number > 90 and energy_number <= 150:
        energy_label = "C"
    elif energy_number > 150 and energy_number <= 230:
        energy_label = "D"
    elif energy_number > 230 and energy_number <= 330:
        energy_label = "E"
    elif energy_number > 330 and energy_number <= 450:
        energy_label = "F"
    elif energy_number > 450:
        energy_label = "G"
    return energy_label