# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser



class MySpider(Spider):
    name = 'dontecho_comadsalquilerinmueble'
    execution_type='testing'
    country='spain'
    locale='es'

    # custom_settings = {
    #     "PROXY_ON": True,
    #     "PASSWORD": "wmkpu9fkfzyo",
    #     "FEED_EXPORT_ENCODING":'utf-8'
    # }

    payload = "{\"currentPage\":next_page,\"itemsPerPage\":20,\"order\":\"desc\",\"orderfield\":\"creationDate\",\"ids\":[],\"showAddress\":1,\"adOperationId\":\"2\",\"adScopeId\":null,\"adTypologyId\":\"0\",\"priceMin\":null,\"priceMax\":null,\"CreationDateMin\":null,\"CreationDateMax\":null,\"locationId\":[],\"drawShapePath\":null,\"homes\":null,\"chalets\":null,\"countryhouses\":null,\"isDuplex\":null,\"isPenthouse\":null,\"isStudio\":null,\"isIndependentHouse\":null,\"isSemidetachedHouse\":null,\"isTerracedHouse\":null,\"constructedAreaMin\":null,\"constructedAreaMax\":null,\"rooms_0\":null,\"rooms_1\":null,\"rooms_2\":null,\"rooms_3\":null,\"rooms_4\":null,\"baths_1\":null,\"baths_2\":null,\"baths_3\":null,\"builtTypeId\":null,\"isTopFloor\":null,\"isIntermediateFloor\":null,\"isGroundFloor\":null,\"isFirstFloor\":null,\"hasAirConditioning\":null,\"hasWardrobe\":null,\"hasGarage\":null,\"hasLift\":null,\"hasTerrace\":null,\"hasBoxRoom\":null,\"hasSwimmingPool\":null,\"hasGarden\":null,\"flatLocationId\":null,\"hasKitchen\":null,\"hasAutomaticDoor\":null,\"hasPersonalSecurity\":null,\"HasSecurity24h\":null,\"garageCapacityId\":null,\"hasHotWater\":null,\"hasExterior\":null,\"hasSuspendedFloor\":null,\"hasHeating\":null,\"isFurnish\":null,\"isBankOwned\":null,\"distributionId\":null,\"isOnlyOfficeBuilding\":null,\"ubicationId\":null,\"warehouseType_1\":null,\"warehouseType_2\":null,\"isATransfer\":null,\"isCornerLocated\":null,\"hasSmokeExtractor\":null,\"landType_1\":null,\"landType_2\":null,\"landType_3\":null,\"HasAllDayAccess\":null,\"HasLoadingDockAccess\":null,\"HasTenant\":null,\"addressVisible\":null,\"mlsIncluded\":null,\"freeText\":null,\"RefereceText\":null,\"isLowered\":null,\"priceDropDateFrom\":0,\"priceDropDateTo\":0,\"arePetsAllowed\":null,\"Equipment\":null,\"OperationStatus\":null,\"AdContract\":null,\"IsRent\":true,\"IsSale\":false,\"AdState\":null}"

    headers = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7',
            'Connection': 'keep-alive',
            'Content-Type': 'application/json; charset=UTF-8'
            }

    def start_requests(self):
        # url = "https://www.dontecho.com/ad/91514630"
        # yield Request(url,callback=self.populate_item,meta={"property_type":"house"})
        start_urls = [
            {"url": "https://www.dontecho.com/ads/alquiler/inmueble/"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.jump)
    def jump(self, response):
        for item in response.xpath("//div[@class='info-results']/h1/a"):
            title = item.xpath("./text()").extract_first()
            if "Piso" in title or "Estudio" in title or "Loft" in title or "Ático" in title:
                follow_url = response.urljoin(item.xpath("./@href").extract_first())
                property_type = "apartment"
                yield Request(follow_url, callback=self.populate_item, meta={'property_type': property_type})
            elif "Dúplex" in title or "Nave" in title or "Local" in title or "Chalet" in title or "Finca" in title:
                follow_url = response.urljoin(item.xpath("./@href").extract_first())
                property_type = "house"
                yield Request(follow_url, callback=self.populate_item, meta={'property_type': property_type})
        
        
        self.payload = self.payload.replace("next_page", "2")
        url = "https://www.dontecho.com/api/AdsSearch/PostSearch"
        yield Request(url, self.parse, method="POST", headers=self.headers, dont_filter=True, body=self.payload)
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 3)
        seen = False
        data = json.loads(response.body)
        content = data.get("PlainTextArray")
        for c in content:
            sel = Selector(text=c, type="html")
            for item in sel.xpath("//div[@class='info-results']/h1/a"):
                title = item.xpath("./text()").extract_first()
                if "Piso" in title or "Estudio" in title or "Loft" in title or "Ático" in title:
                    follow_url = response.urljoin(item.xpath("./@href").extract_first())
                    property_type = "apartment"
                    yield Request(follow_url, callback=self.populate_item, meta={'property_type': property_type})
                    
                elif "Dúplex" in title or "Nave" in title or "Local" in title or "Chalet" in title or "Finca" in title:
                    follow_url = response.urljoin(item.xpath("./@href").extract_first())
                    property_type = "house"
                    yield Request(follow_url, callback=self.populate_item, meta={'property_type': property_type})
                seen = True

        
        if page < 10:
           
            self.payload = self.payload.replace("next_page", str(page))
            
            url = "https://www.dontecho.com/api/AdsSearch/PostSearch"
            yield Request(url, self.parse, method="POST", dont_filter=True, headers=self.headers, body=self.payload, meta={"page" : page +1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Dontecho_comadsalquilerinmueble_PySpider_"+ self.country + "_" + self.locale)
        item_loader.add_xpath("title", "//h1[@id='titulo']/text()")
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        price = "".join(response.xpath("//li/span[@class='price']/text()").extract())
        if price:
            item_loader.add_value("rent_string", price)

        meters = "".join(response.xpath("//section[@class='encabezado']/ul/li[contains(.,'m²')]/text()").extract())
        if meters:
            item_loader.add_value("square_meters", meters.split("m²")[0].strip())

        images = []
        img = "".join(response.xpath("//script[@type='text/javascript']/text()[contains(.,'src')]").extract())
        image =  "["+img.split("'[")[1].split("]'")[0]+"]"
        json_l = json.loads(image)
        for j in json_l:
            img = j.get("src")
            images.append(img)
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))

        address =  "".join(response.xpath("//h1[@id='titulo']/text()").extract())
        if address:
            item_loader.add_value("address", address.split("alquiler")[1])
        
        item_loader.add_value("city", "Marid")

        room_count = "".join(response.xpath("//section[@class='encabezado']/ul/li[contains(.,'Dorm')]/text()").extract())
        if room_count:
            item_loader.add_value("room_count", room_count.split("Dorm")[0].strip())

        desc = "".join(response.xpath("//section[@class='encabezado']/p/text()").extract())
        item_loader.add_value("description", desc.strip())

        bathroom_count = response.xpath("//li[contains(.,'Baño')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip().split(' ')[0].strip())
        
        if desc:
            if 'disponible a partir del' in desc:
                available_date = desc.lower().split('disponible a partir del')[-1].split('.')[0].strip()
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%B/%Y"], languages=['es'])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        floor = response.xpath("//li[contains(.,'Planta')]/text()").get()
        if floor and 'exterior' in floor:
            item_loader.add_value("floor", floor.split('Planta')[-1].split(',')[0].strip())

        deposit = response.xpath("//div[@id='caracteristicas']//li[contains(.,'Fianza')]/text()").get()
        if deposit:
            try:
                deposit_value = deposit.split("Fianza")[1].split("mes")[0].strip()
                if deposit_value.isdigit():
                    deposit = int(deposit_value)*int(price.split("€")[0].replace(".","").strip())
                    item_loader.add_value("deposit", deposit)
            except:
                pass
            
        latitude_longitude = response.xpath("//input[@name='defaultLatLng']/@value").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('ltd: ')[1].split(',')[0].strip()
            longitude = latitude_longitude.split('lng: ')[1].split('}')[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)

        pets_allowed = "".join(response.xpath("//br[contains(following-sibling::text(), 'mascotas')]/following-sibling::text()[1] | //br[contains(following-sibling::text(), 'Mascotas')]/following-sibling::text()[1] ").getall())
        if pets_allowed:
            if 'no se admiten' in pets_allowed.lower():
                item_loader.add_value("pets_allowed", False)
            elif 'se admiten' in pets_allowed.lower():
                item_loader.add_value("pets_allowed", True)
            elif "mascotas permitidas" in pets_allowed.lower():
                item_loader.add_value("pets_allowed", True)

        swimming_pool = response.xpath("//li[contains(.,'Piscina')]").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)

        external_id = "".join(response.xpath("//span[@class='property-ref']/text()").extract())
        if external_id:
            item_loader.add_value("external_id", external_id.split(".")[1].strip())     

        energy_label = "".join(response.xpath("//div[@class='caracteristicas']/ul/li[contains(.,'energética')]/text()").extract())
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split(" ")[-1].strip())

        terrace = "".join(response.xpath("//div[@class='caracteristicas']/ul/li[contains(.,'Terraza')]/text()").extract())
        if terrace:
            item_loader.add_value("terrace", True)


        terrace = "".join(response.xpath("//div[@class='caracteristicas']/ul/li[contains(.,'ascensor')]/text()").extract())
        if terrace:
            item_loader.add_value("elevator", True)

        terrace = "".join(response.xpath("//div[@class='caracteristicas']/ul/li[contains(.,'casa amueblada')]/text()").extract())
        if terrace:
            item_loader.add_value("furnished", True)
        else:
            terrace = "".join(response.xpath("//div[@class='caracteristicas']/ul/li[contains(.,'casa sin amueblar')]/text()").extract())
            if terrace:
                item_loader.add_value("furnished", False)


        parking = "".join(response.xpath("//div[@class='caracteristicas']/ul/li[contains(.,'garaje')]/text()").extract())
        if parking:
            item_loader.add_value("parking", True)


        item_loader.add_value("landlord_phone", "952 082 321")
        item_loader.add_value("landlord_email", "info@dontecho.com")
        item_loader.add_value("landlord_name", "DON TECHO Grupo Inmobiliario")



        yield item_loader.load_item()