from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'guyhoquet_paris15_sudconvention_com'
    execution_type='testing'
    country='france'
    locale='fr'
    headers = {
        # 'content-type': "application/json; charset=utf-8",
        'accept': "*/*",
        'accept-encoding': "gzip, deflate, br",
        'accept-language': "tr-TR,tr;q=0.9,en-GB;q=0.8,en;q=0.7,en-US;q=0.6",
        'referer': "https://www.guy-hoquet.com/biens/result",
        'sec-fetch-dest': "empty",
        'sec-fetch-mode': "cors",
        'sec-fetch-site': "same-origin",
        'x-csrf-token': "wjBv1He9RB0qeVQEAAh0uiAWDpflWeaHLRbpmO40",
        'x-requested-with': "XMLHttpRequest",
        'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36",
    }
    

    def start_requests(self):
        start_urls = [
           
            {"url": "https://www.guy-hoquet.com/biens/result?1&templates%5B%5D=properties&p=1&t=&filters%5B10%5D%5B%5D=2&filters%5B30%5D%5B%5D=appartement&with_markers=false&with_map=true&_=1636699098625", "property_type": "apartment"},
	        {"url": "https://www.guy-hoquet.com/biens/result?1&templates%5B%5D=properties&p=1&t=&filters%5B10%5D%5B%5D=2&filters%5B30%5D%5B%5D=maison&with_markers=false&with_map=true&_=1636699573791", "property_type": "house"},
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
        
        desc="".join(response.xpath("//span[@class='description-more']/text()").getall())
        if "pascale rocheron" in desc.lower():
            return
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))

        item_loader.add_value("external_source", "Guyhoquet_Immobilier_Paris_15_PySpider_france")

        item_loader.add_xpath("title", "//div/h1/text()")

        rent="".join(response.xpath("//div[@class='price']/text()").getall())
        if rent:
            item_loader.add_value("rent_string", rent.replace(" ",""))
        
        square_meters="".join(response.xpath("//div[@class='biens-list']//div/i[contains(@class,'ico scale')]/parent::div//text()").getall())
        if square_meters:
            meters = square_meters.split('m²')[0].strip()
            item_loader.add_value("square_meters", int(float(meters)))
        
        room_count="".join(response.xpath("//div[@class='biens-list']//div/i[contains(@class,'ico room')]/parent::div//text()").getall())
        if room_count:
            item_loader.add_value("room_count", room_count.strip().split(' ')[0])
        
        bathroom_count="".join(response.xpath("//div[@class='biens-list']//div/i[contains(@class,'ico bath') or contains(@class,'ico shower')]/parent::div//text()").getall())
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip().split(' ')[0])
        
        address = response.xpath("//div[@class='add']/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", " ".join(address.split(" ")[:-1]))
            item_loader.add_value("zipcode", address.split(" ")[-1])
        
        latitude_longitude = response.xpath("//script[@type='text/javascript'][contains(.,'Lat')]/text()").get()
        if latitude_longitude:
                latitude = latitude_longitude.split(" Lat = '")[1].split("';")[0]
                longitude = latitude_longitude.split("Lng = '")[1].split("';")[0]
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
        
        utilties=response.xpath("//div[@class='horaires-item']/div/div[contains(.,'Provision sur charges')]//following-sibling::div/text()").get()
        if utilties:
            item_loader.add_value("utilities", utilties.split('€')[0].strip())
        
        deposit = response.xpath("//div[@class='horaires-item']/div/div[contains(.,'de garantie')]//following-sibling::div/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split('€')[0].strip())
        
        furnished = response.xpath("//div[@class='horaires-item']/div/div[contains(.,'Meubl')]//following-sibling::div/text()").get()
        if furnished:
            if "Oui" in furnished:
                item_loader.add_value("furnished", True)
            if "-meuble-" in response.url and "-non-meuble-" not in response.url:
                item_loader.add_value("furnished", True)
            if "Non" in furnished:
                item_loader.add_value("furnished", False)
        
        parking="".join(response.xpath("//div[@class='biens-list']//div/i[contains(@class,'parking')]/parent::div//text()").getall())
        if parking:
            item_loader.add_value("parking", True)

        balcony = "".join(response.xpath("//div[@class='horaires-item']/div/div[contains(.,'balcon')]//following-sibling::div/text()").getall())
        if balcony:
            item_loader.add_value("balcony", True)

        item_loader.add_value("landlord_name", "GUY HOQUET PARIS 15 SUD CONVENTION")
        item_loader.add_value("landlord_phone", "01 45 32 40 40")
     
        yield item_loader.load_item()