import scrapy
from python_spiders.loaders import ListingLoader


class BenecaseSpider(scrapy.Spider):
    name = 'benecase'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['benecase.it']
    start_urls = ['http://benecase.it/affitto.html#f2c_item_count']

    def parse(self, response):
        for page in response.css(".span4.categbox a::attr(href)").getall():
            yield scrapy.Request(url=response.urljoin(page), callback=self.parse_page)


    def parse_page(self, response):
        title = response.css(".carousel-caption h1::text").get()
        address = response.css(".carousel-caption h1::text").get()
        rent = response.css(".carousel-caption div.prez::text").re("[0-9]+\.*[0-9]+")
        landlord_name = response.css("meta[name='author']::attr(content)").get()
        images = response.css(".item img::attr(src)").getall()
        floor_plan_images = [response.urljoin(i) for i in response.css("a.jcepopup::attr(href)").getall()]
        floor = response.xpath('//ol/li[contains(text(),"FLOOR")]/text()').re("[0-9]")
        elevator = response.xpath('//ol/li[contains(text(),"ELEVATOR")]/text()').get()
        square_meters = response.xpath('//ol/li/text()').re("[0-9]+\W*M2")
        room_count = response.xpath('//ol/li/text()').re("[0-9]+\W*CAMER")
        bathroom_count = response.xpath('//ol/li/text()').re("[0-9]+\W*BAGNI")
        energy_label = response.xpath('//ol/li[contains(text(),"PERFORMANCE: ")]/text()').get()
        
        if square_meters:
            square_meters = int(square_meters[0].replace("M2","").strip())

        if floor:
            floor = floor[0]

        if room_count:
            room_count = int(room_count[0].replace("CAMER","").strip())


        if bathroom_count:
            bathroom_count = int(bathroom_count[0].replace("BAGNI","").strip())
        if energy_label:
            energy_label = energy_label.split(":")[1].strip()
        if elevator:
            if 'senza' in elevator.lower():
                elevator = False
            else:
                elevator = True
        if rent:
            rent = int(rent[0].replace(".",""))

            item = ListingLoader(response=response)
            item.add_value("external_source"        ,self.external_source)
            item.add_value("external_link"          ,response.url)
            item.add_value("title"                  ,title)
            item.add_value("address"                ,address)
            item.add_value("landlord_name"          ,landlord_name)
            item.add_value("rent"                   ,rent)
            item.add_value("square_meters"          ,square_meters)
            item.add_value("room_count"             ,room_count)
            item.add_value("bathroom_count"         ,bathroom_count)
            item.add_value("images"                 ,images)
            item.add_value("floor_plan_images"      ,floor_plan_images)
            item.add_value("elevator"               ,elevator)
            item.add_value("energy_label"           ,energy_label)
            item.add_value("floor"                  ,floor)
            item.add_value("currency"               ,"EUR")
            item.add_value("landlord_phone"         ,"06.68.96.830")
            item.add_value("property_type"          ,"apartment")
            item.add_value("city"                   ,"roma")
            yield item.load_item()

