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
    name = 'lechesnay_guy_hoquet_com'
    execution_type='testing'
    country='france'
    locale='fr' 
    headers = {
        'content-type': "application/json; charset=utf-8",
        'accept': "*/*",
        'accept-encoding': "gzip, deflate, br",
        'accept-language': "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
        'referer': "https://www.guy-hoquet.com/biens/result",
        'sec-fetch-dest': "empty",
        'sec-fetch-mode': "cors",
        'sec-fetch-site': "same-origin",
        'x-csrf-token': "GuGCmClg4JCZMfDCsD9dFndkTsVwpX7nrKC99OWP",
        'x-requested-with': "XMLHttpRequest",
        
    }
    

    def start_requests(self):
        start_urls = [
           
            {"url": "https://www.guy-hoquet.com/biens/result?1&templates%5B%5D=properties&p=1&t=&filters%5B10%5D%5B%5D=2&filters%5B20%5D%5B%5D=le-chesnay-78150_c3&filters%5B30%5D%5B%5D=appartement&with_markers=false&with_map=true&_=1631692819060", "property_type": "apartment"},
	        {"url": "https://www.guy-hoquet.com/biens/result?1&templates%5B%5D=properties&p=1&t=&filters%5B10%5D%5B%5D=2&filters%5B20%5D%5B%5D=le-chesnay-78150_c3&filters%5B30%5D%5B%5D=maison&with_markers=false&with_map=true&_=1631692908253", "property_type": "house"},
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
        print(data)
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
        
        desc="".join(response.xpath("//span[@class='description-more']/text()").getall())
        if "pascale rocheron" in desc.lower():
            return
        
        item_loader.add_value("external_source", "Lechesnay_guy_hoquet_com_PySpider_"+ self.country + "_" + self.locale)
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_xpath("title", "//div/h1/text()")
        
        
        rent = response.xpath("//div[@class='container']/div[@class='price']/text()[1]").get()
        if rent:
            rent = rent.split('€')[0].strip().replace('\xa0', '').replace(' ', '')
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", 'EUR')
        
        square_meters = response.xpath("//div[@class='no-desk']//i[contains(@class,'scale')]/following-sibling::div/text()").get()
        if square_meters:
            square_meters = str(int(float(square_meters.split('m')[0].strip())))
            item_loader.add_value("square_meters", square_meters)
        
        room_count="".join(response.xpath(
            "//div[@class='biens-list']//div/i[contains(@class,'ico room')]/parent::div//text()").getall())
        if room_count:
            item_loader.add_value("room_count", room_count.strip().split(' ')[0])
        
        latitude_longitude = response.xpath("//script[@type='text/javascript'][contains(.,'Lat')]/text()").get()
        if latitude_longitude:
                latitude = latitude_longitude.split(" Lat = '")[1].split("';")[0]
                longitude = latitude_longitude.split("Lng = '")[1].split("';")[0]
                if latitude and longitude:
                    item_loader.add_value("longitude", longitude)
                    item_loader.add_value("latitude", latitude)
        

        external_id=response.xpath("//div[@class='code']/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1].strip())
    
        desc="".join(response.xpath("//span[@class='description-more']/text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
        
        floor=response.xpath("//div[@class='horaires-item']/div/div[contains(.,'Etage')]//following-sibling::div/text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())
            
        images=[x for x in response.xpath("//div[@class='de-biens-slider']/div/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        utilities=response.xpath(
            "//p[contains(@class,'acquirer_honorary_included')]//text()").get()
        if utilities:
            utilities = utilities.split(":")[1].split("€")[0].strip()
            item_loader.add_value("utilities", utilities)

        address=response.xpath("//div[@class='add']/text()").get()
        if address:
            item_loader.add_value("address",address)
            zipcode = address.split(" ")[-1]
            item_loader.add_value("city",address.replace(zipcode,"").strip())
            item_loader.add_value("zipcode",zipcode)
              
        deposit="".join(response.xpath("//div[div[.='Dépôt de garantie']]/div[2]/text()").getall())
        if deposit:
            deposit = deposit.split("€")[0].strip()
            item_loader.add_value("deposit", int(float(deposit))) 
        bathroom=response.xpath("normalize-space(//div[@class='biens-item']/div/i[@class='ico bath']/following-sibling::div/text())").get()
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom.split(" ")[0].strip())  
        parking=response.xpath("normalize-space(//div[@class='biens-item']/div/i[@class='ico parking']/following-sibling::div/text())").get()
        if parking:
            item_loader.add_value("parking", True)
        furnished="".join(response.xpath("//div[@class='horaires-item']/div/div[contains(.,'Meublé')]//following-sibling::div/text()").getall())
        if furnished:
            if "non" in furnished.lower():
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)
        item_loader.add_value("landlord_name", "GUY HOQUET LE CHESNAY")        
        item_loader.add_value("landlord_phone", "01 83 43 60 60")
        yield item_loader.load_item()