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
    name = 'saintegenevievedesbois_guyhoquet_com'
    execution_type='testing'
    country='france'
    locale='fr'

    headers = {
        'content-type': "application/json; charset=utf-8",
        'accept': "*/*",
        'accept-encoding': "gzip, deflate, br",
        'accept-language': "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
        'referer': "https://sainte-genevieve-des-bois.guy-hoquet.com/biens/result",
        'sec-fetch-dest': "empty",
        'sec-fetch-mode': "cors",
        'sec-fetch-site': "same-origin",
        'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36",
        'x-csrf-token': "AA19ZTqxDeAKKSNEG1rrgHuPBdBIlF9HrcMBeTs6",
        'x-requested-with': "XMLHttpRequest",
        'cache-control': "no-cache",
    }
    

    def start_requests(self):
        start_urls = [
            {"url": "https://sainte-genevieve-des-bois.guy-hoquet.com/biens/result?1&templates%5B%5D=properties&p=1&t=&filters%5B10%5D%5B%5D=2&filters%5B30%5D%5B%5D=appartement&with_markers=false&_=1603547196095", "property_type": "apartment"},
	        {"url": "https://sainte-genevieve-des-bois.guy-hoquet.com/biens/result?1&templates%5B%5D=properties&p=1&t=&filters%5B10%5D%5B%5D=2&filters%5B30%5D%5B%5D=maison&with_markers=false&_=1603546641822", "property_type": "house"},
        ] 
        for url in start_urls:
            yield Request(url=url.get('url'),
                            headers=self.headers,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        
        data=json.loads(response.body)
        data_html = data["templates"]["properties"]
        sel = Selector(text=data_html, type="html")

        page = response.meta.get('page', 2)
        seen = False
        data_url=sel.xpath("//div[contains(@class,'resultat-item')]/a/@href").extract()
        for item in data_url:
            follow_url = response.urljoin(item)
            print(follow_url)
            yield Request(follow_url, callback=self.populate_item ,dont_filter=True, meta={'property_type': response.meta.get('property_type')})
            seen = True
        if response.meta.get('property_type')=="apartment":
            if page == 2 or seen:
                url = f"https://sainte-genevieve-des-bois.guy-hoquet.com/biens/result?1&templates%5B%5D=properties&p={page}&t=&filters%5B10%5D%5B%5D=2&filters%5B30%5D%5B%5D=appartement&with_markers=false&_=1603547196095"
                yield Request(url, callback=self.parse, headers=self.headers, dont_filter=True, meta={"page": page+1 ,'property_type': response.meta.get('property_type')})
        
        if response.meta.get('property_type')=="house":
            if page == 2 or seen:
                url = f"https://sainte-genevieve-des-bois.guy-hoquet.com/biens/result?1&templates%5B%5D=properties&p={page}&t=&filters%5B10%5D%5B%5D=2&filters%5B30%5D%5B%5D=maison&with_markers=false&_=1603546641822"
                yield Request(url, callback=self.parse, headers=self.headers, dont_filter=True, meta={"page": page+1 ,'property_type': response.meta.get('property_type')})
     
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        desc="".join(response.xpath("//span[@class='description-more']/text()").getall())
        if "pascale rocheron" in desc.lower():
            return
            
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Saintegenevievedesbois_guyhoquet_PySpider_"+ self.country + "_" + self.locale)

        item_loader.add_xpath("title", "//div/h1/text()")
        
        rent="".join(response.xpath("//div[@class='price']/text()").getall())
        if rent:
            item_loader.add_value("rent_string", rent.replace(" ",""))
         
        square_meters="".join(response.xpath("//div[@class='biens-list']//div/i[contains(@class,'ico scale')]/parent::div//text()").getall())
        if square_meters:
            s_meters = square_meters.split('m²')[0].strip()
            item_loader.add_value("square_meters", int(float(s_meters)))

        room_count="".join(response.xpath("//div[@class='biens-list']//div/i[contains(@class,'ico room')]/parent::div//text()").getall())
        if room_count:
            item_loader.add_value("room_count", room_count.strip().split(' ')[0])
        bathroom = "".join(response.xpath("//div[@class='horaires-item']//div[contains(.,'eau') and contains(.,'Salle')]/following-sibling::div/text()").getall())
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom.strip())
        latitude_longitude = response.xpath("//script[@type='text/javascript'][contains(.,'Lat')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split(" Lat = '")[1].split("';")[0]
            longitude = latitude_longitude.split("Lng = '")[1].split("';")[0]
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        address = response.xpath("//div[@class='container']/div[@class='add']//text()").get()
        if address:
            item_loader.add_value("address", address)
            try:
                zipcode = address.strip().split(" ")[-1]
                if zipcode.isdigit():
                    item_loader.add_value("zipcode", zipcode)
                    item_loader.add_value("city", address.replace(zipcode,""))
            except:
                pass            
           
        external_id=response.xpath("//div[@class='code']/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1].strip())

        desc="".join(response.xpath("//span[@class='description-more']/text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
        
        floor=response.xpath("//div[@class='horaires-item']//div[contains(.,'Etage')]/following-sibling::div/text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())
            
        images=[x for x in response.xpath("//div[@class='de-biens-slider']/div/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            
        deposit=response.xpath("//div[@class='horaires-item']//div[contains(.,'Dépôt de garantie')]/following-sibling::div/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split('€')[0].strip())

        utilities = response.xpath("//div[@class='horaires-item']//div[contains(.,'charge')]/following-sibling::div/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split('€')[0].strip())

        swimming_pool = response.xpath("//div[@class='horaires-item']//div[contains(.,'Piscine')]/following-sibling::div/text()").extract_first()
        if swimming_pool:
            if "Non" in swimming_pool:
                item_loader.add_value("swimming_pool", False)
            else:
                item_loader.add_value("swimming_pool", True)
        
        parking = response.xpath("//div[@class='horaires-item']//div[contains(.,'parking')]/following-sibling::div/text()").extract_first()
        if parking:
            if "Non" in parking:
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)

        furnished = response.xpath("//div[@class='horaires-item']//div[contains(.,'Meublé')]/following-sibling::div/text()").extract_first()
        if furnished:
            if "Non" in furnished:
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)
        
        energy_label = response.xpath("//div[@class='dpe']/img[contains(@data-src,'dpe')]/following-sibling::div[@class='dpeValue']//text()").extract_first()
        if energy_label:
            item_loader.add_value("energy_label",energy_label_calculate(energy_label))

        item_loader.add_value("landlord_name", "Guy Hoquet SAINTE GENEVIEVE DES BOIS")        
        item_loader.add_value("landlord_phone", "01 60 15 09 85")
        
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