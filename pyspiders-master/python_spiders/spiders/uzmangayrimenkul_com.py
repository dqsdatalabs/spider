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
    name = 'uzmangayrimenkul_com'    
    execution_type='testing'
    country='turkey'
    locale='tr'   
    external_source = "Uzmangayrimenkul_PySpider_turkey_tr"
    def start_requests(self):
        start_urls = [
            {
                "url" : ["https://uzmangayrimenkul.com.tr/tr/liste.php?page=15&oda_sayisi=&bina_yasi=&subeadi1=&oda1=&oda2=99&cazip1=&kiratipi1=&ks1=KIRALIK&portfoytipi1=daire&guney1=&kuzey1=&dogu1=&bati1=&fiyat1=&fiyat2=99999999&metrekare1=&metrekare2=99999&semt1=&ilce1=&sehir1=Adana&cevre_duzeni1=&giris_kapisi1=&zemin1=&ic_kapilar1=&pencere1=&mutfak1=&foto=adana-huzurevleri-mah.-kiralik-daire-16285-.jpg&ilan_tarihi=&Listele=&demir_panjur_kapi1=&ebeveyn_banyosu1=&banyo_hilton_lavabo1=&kuvet1=&jakuzi_motorlu1=&dusakabin1=&gunes_enerjisi1=&yuzme_havuzu1=&jenerator1=&somine1=&deniz_manzarali1=&bogaz_manzarali1=&asansor1=&plastik_boya1=&alci_siva1=&saten_boya1=&kartonpiyer1=&asma_tavan1=&spot_lamba1=&gomme_dolap1=&demir_sebeke1=&kombi1=&kalorifer_tesisati1=&kalorifer1=&ankastre_firin1=&ankastre_davlumbaz1=&klima_tesisati1=&split_klima1=&diyafon1=&yangin_merdiveni1=&otopark_acik1=&otopark_kapali1=&guvenlik1=&aspirator1=&sofben1=&isicam1=&barbeku1=&kapici1=&gol_manzarasi1=&gol_gorur1=&tek_blok1=&vestiyer1=&site_icerisinde1=&krediye_uygun1=&kelime=&odasayisi1=&odasayisi2=&odasayisi3=&odasayisi4=&odasayisi5=&odasayisi6=&odasayisi7="],
                "property_type" : "apartment",
                "city" : "Adana",
            },
            {
                "url" : ["https://uzmangayrimenkul.com.tr/tr/ilan-listesi?il1=Adana&ilce1=&semt1=&fiyat1=&fiyat2=99999999&metrekare1=&metrekare2=&bina_yasi1=&site_icerisinde1=&krediye_uygun1=&ks1=KIRALIK&portfoytipi1=M%FCstakil"],
                "property_type" : "house",
                "city" : "Adana",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "city": url.get("city")})


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='resim']/../@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type'), "city":response.meta.get("city")})
    
        next_page = response.xpath("//i[.='İleri']/../@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type'), "city":response.meta.get("city")}
            )
            
        

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("external_id","//div[@class='detayOzellikler']//li[contains(.,'İlan No')]/span/text()")
        title = response.xpath("//div[@class='headTitle']/h1/text()").extract_first()
       
        if title:
            title=title.replace("\u00c7","").replace("\u015e","").replace("\u0130","").replace("\u00dc","").replace("\u00d6","").replace("\u011e","")
            item_loader.add_value("title",title)
            if "İŞYERİ" in title:
                return
            if "EŞYA" in title.upper():
                item_loader.add_value("furnished",True)
    
        rent = response.xpath("//div[@class='headPrice']/text()").extract_first().replace(",",".")
        if rent:
            item_loader.add_value("rent_string", rent.strip())
        
        item_loader.add_value("city", response.meta.get("city"))
       
        square_meters = response.xpath("//div[@class='detayOzellikler']//li[contains(.,'Metrekare')]/span/text()").extract_first()
        if square_meters :
            item_loader.add_value("square_meters",square_meters.split("m")[0])
        
        images = [response.urljoin(x) for x in response.xpath("//div[@class='tn3 album']//img/@src").extract()]
        if images:
                item_loader.add_value("images", images)      

        description="".join(response.xpath("//div[@class='detayaciklama']/p//text()").extract())
        if description:
            item_loader.add_value("description",description.strip().replace("\u00c7","").replace("\u00a0","").replace("\u0130","").replace("\u015e","").replace("\u00dc","").replace("\u011e","").replace("\u00d6","").replace("\u0130","").replace("\u015e","").replace("\u00c7","").replace("\u011e","").replace("\n","").replace("\t",""))

        address=response.xpath("//div[@class='container']/ul/li[last()]//text()").get()
        if address:
            item_loader.add_value("address", address.replace("\u011e","").replace("\u015e","").replace("\u0130","").replace("\u00dc",""))
        else:
            item_loader.add_value("address", response.meta.get("city"))

        item_loader.add_xpath("bathroom_count","//div[@class='detayOzellikler']//li[contains(.,'Banyo Sayısı')]/span/text()[.!='0']")
        
        room = response.xpath("//div[@class='detayOzellikler']//li[contains(.,'Oda Sayısı')]/span/text()").extract_first()
        if room:
                add=0
                room_array=room.split("+")
                for i in room_array:
                    add += int(i)
                item_loader.add_value("room_count",str(add) )
        floor = response.xpath("//div[@class='detayOzellikler']//li[contains(.,'Kat')]/span/text()").extract_first()
        if floor:          
            item_loader.add_value("floor", floor.split("/")[1].split())

        parking=response.xpath("//div[@class='detaySol']//li[contains(.,'Otopark')]//text()").extract_first()
        if parking:
            item_loader.add_value("parking",True)
        
        elevator=response.xpath("//div[@class='detaySol']//li[contains(.,'Asansör')]//text()").extract_first()
        if elevator:
            item_loader.add_value("elevator",True)
   
        item_loader.add_xpath("landlord_name","//div[@class='ilanDanisman']/h2[@class='name']/text()")
        email=response.xpath("//div[@class='ilanDanisman']//li[@class='email']/text()").get()
        if email:
            item_loader.add_value("landlord_email", email)
        else:
            item_loader.add_value("landlord_email", "info@uzmangayrimenkul.com.tr")

        item_loader.add_xpath("landlord_phone","//div[@class='ilanDanisman']//li[@class='phone']/text()")
  

        yield item_loader.load_item()

        
       

        
        
          

        

      
     